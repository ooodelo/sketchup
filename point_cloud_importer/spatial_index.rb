# frozen_string_literal: true

module PointCloudImporter
  # A lightweight KD-tree for nearest neighbor lookup.
  class SpatialIndex
    Node = Struct.new(:point, :index, :axis, :left, :right, :bucket)

    DEFAULT_MAX_DEPTH = 50
    CACHE_VERSION = 1

    class << self
      def deserialize(payload)
        return unless payload.is_a?(Hash)
        return unless payload['version'] == CACHE_VERSION

        instance = allocate
        instance.send(:initialize_from_serialized, payload)
        instance
      rescue StandardError
        nil
      end
    end

    def initialize(points, indices, max_depth: DEFAULT_MAX_DEPTH)
      @max_depth = max_depth
      data = points.each_with_index.map { |point, local_index| [point, indices[local_index]] }
      @root = build(data)
    end

    def serialize
      {
        'version' => CACHE_VERSION,
        'max_depth' => @max_depth,
        'root' => serialize_node(@root)
      }
    end

    def nearest(target)
      return unless @root

      best = nil
      search(@root, target, best)
    end

    private

    def initialize_from_serialized(payload)
      @max_depth = payload['max_depth'] || DEFAULT_MAX_DEPTH
      @root = deserialize_node(payload['root'])
    end

    def build(data)
      return nil if data.empty?

      root = nil
      stack = [[nil, :root, 0, data.length, 0]]

      until stack.empty?
        parent, direction, start_idx, end_idx, depth = stack.pop
        next if start_idx >= end_idx

        node, median_idx = if depth >= @max_depth
                              [Node.new(nil, nil, nil, nil, nil, build_bucket(data, start_idx, end_idx)), nil]
                            else
                              build_internal_node(data, start_idx, end_idx, depth)
                            end

        if parent
          parent.public_send("#{direction}=", node)
        else
          root = node
        end

        next if node.bucket

        left_start = start_idx
        left_end = median_idx
        right_start = median_idx + 1
        right_end = end_idx

        stack << [node, :right, right_start, right_end, depth + 1] if right_start < right_end
        stack << [node, :left, left_start, left_end, depth + 1] if left_start < left_end
      end

      root
    end

    def build_internal_node(data, start_idx, end_idx, depth)
      axis = choose_axis(data, start_idx, end_idx, depth)
      median_idx = start_idx + ((end_idx - start_idx) / 2)
      quickselect!(data, start_idx, end_idx, median_idx, axis)
      entry = data[median_idx]

      [Node.new(entry[0], entry[1], axis, nil, nil, nil), median_idx]
    end

    def search(node, target, best)
      return best unless node

      if node.bucket
        node.bucket.each do |point, index|
          current_distance = squared_distance(point, target)
          if best.nil? || current_distance < best[:distance]
            best = { point: point, index: index, distance: current_distance }
          end
        end
        return best
      end

      axis = node.axis
      value = coordinate(target, axis)
      node_value = coordinate(node.point, axis)

      next_branch = value < node_value ? node.left : node.right
      opposite_branch = value < node_value ? node.right : node.left

      best = search(next_branch, target, best)

      current_distance = squared_distance(node.point, target)
      if best.nil? || current_distance < best[:distance]
        best = { point: node.point, index: node.index, distance: current_distance }
      end

      axis_distance = (value - node_value)**2
      if opposite_branch && (best.nil? || axis_distance < best[:distance])
        best = search(opposite_branch, target, best)
      elsif best.nil? && opposite_branch
        best = search(opposite_branch, target, best)
      end

      best
    end

    def build_bucket(data, start_idx, end_idx)
      bucket = []
      start_idx.upto(end_idx - 1) do |index|
        bucket << data[index]
      end
      bucket
    end

    def build_bounds(data, start_idx, end_idx)
      mins = [Float::INFINITY, Float::INFINITY, Float::INFINITY]
      maxs = [-Float::INFINITY, -Float::INFINITY, -Float::INFINITY]

      start_idx.upto(end_idx - 1) do |idx|
        point = data[idx][0]
        mins[0] = point.x if point.x < mins[0]
        mins[1] = point.y if point.y < mins[1]
        mins[2] = point.z if point.z < mins[2]
        maxs[0] = point.x if point.x > maxs[0]
        maxs[1] = point.y if point.y > maxs[1]
        maxs[2] = point.z if point.z > maxs[2]
      end

      [mins, maxs]
    end

    def choose_axis(data, start_idx, end_idx, depth)
      mins, maxs = build_bounds(data, start_idx, end_idx)

      best_axis = depth % 3
      best_cost = Float::INFINITY

      3.times do |axis|
        axis_min = mins[axis]
        axis_max = maxs[axis]
        next if axis_max == axis_min

        plane = (axis_min + axis_max) * 0.5
        left_min = [Float::INFINITY, Float::INFINITY, Float::INFINITY]
        left_max = [-Float::INFINITY, -Float::INFINITY, -Float::INFINITY]
        right_min = [Float::INFINITY, Float::INFINITY, Float::INFINITY]
        right_max = [-Float::INFINITY, -Float::INFINITY, -Float::INFINITY]
        left_count = 0
        right_count = 0

        start_idx.upto(end_idx - 1) do |idx|
          point = data[idx][0]
          coord = coordinate(point, axis)
          if coord <= plane
            left_count += 1
            update_bounds(left_min, left_max, point)
          else
            right_count += 1
            update_bounds(right_min, right_max, point)
          end
        end

        if left_count.zero? || right_count.zero?
          if best_cost.infinite?
            best_axis = axis
          end
          next
        end

        left_area = bounding_box_area(left_min, left_max)
        right_area = bounding_box_area(right_min, right_max)
        cost = (left_count * left_area) + (right_count * right_area)

        if cost < best_cost
          best_cost = cost
          best_axis = axis
        end
      end

      if best_cost.infinite?
        ranges = maxs.zip(mins).map { |max, min| max - min }
        best_axis = ranges.each_with_index.max_by { |range, _idx| range }&.last || best_axis
      end

      best_axis
    end

    def update_bounds(mins, maxs, point)
      mins[0] = point.x if point.x < mins[0]
      mins[1] = point.y if point.y < mins[1]
      mins[2] = point.z if point.z < mins[2]
      maxs[0] = point.x if point.x > maxs[0]
      maxs[1] = point.y if point.y > maxs[1]
      maxs[2] = point.z if point.z > maxs[2]
    end

    def bounding_box_area(mins, maxs)
      dx = maxs[0] - mins[0]
      dy = maxs[1] - mins[1]
      dz = maxs[2] - mins[2]
      dx = 0.0 if !dx.finite? || dx.negative?
      dy = 0.0 if !dy.finite? || dy.negative?
      dz = 0.0 if !dz.finite? || dz.negative?
      2.0 * ((dx * dy) + (dy * dz) + (dx * dz))
    end

    def quickselect!(data, left, right_exclusive, n, axis)
      right = right_exclusive - 1
      while left <= right
        pivot_index = partition(data, left, right, (left + right) / 2, axis)
        return data[n] if n == pivot_index

        if n < pivot_index
          right = pivot_index - 1
        else
          left = pivot_index + 1
        end
      end

      data[n]
    end

    def partition(data, left, right, pivot_index, axis)
      pivot_value = coordinate(data[pivot_index][0], axis)
      swap(data, pivot_index, right)
      store_index = left

      left.upto(right - 1) do |idx|
        value = coordinate(data[idx][0], axis)
        next unless value < pivot_value

        swap(data, store_index, idx)
        store_index += 1
      end

      swap(data, right, store_index)
      store_index
    end

    def swap(array, i, j)
      array[i], array[j] = array[j], array[i]
    end

    def serialize_node(node)
      return unless node

      if node.bucket
        {
          'bucket' => node.bucket.map { |point, index| [[point.x, point.y, point.z], index] }
        }
      else
        {
          'axis' => node.axis,
          'index' => node.index,
          'point' => [node.point.x, node.point.y, node.point.z],
          'left' => serialize_node(node.left),
          'right' => serialize_node(node.right)
        }
      end
    end

    def deserialize_node(payload)
      return unless payload

      if payload['bucket']
        bucket = payload['bucket'].map do |coords, index|
          [Geom::Point3d.new(*coords), index]
        end
        Node.new(nil, nil, nil, nil, nil, bucket)
      else
        point = payload['point']
        node = Node.new(Geom::Point3d.new(*point), payload['index'], payload['axis'])
        node.left = deserialize_node(payload['left']) if payload.key?('left')
        node.right = deserialize_node(payload['right']) if payload.key?('right')
        node
      end
    end

    def coordinate(point, axis)
      case axis
      when 0 then point.x
      when 1 then point.y
      else point.z
      end
    end

    def squared_distance(a, b)
      (a.x - b.x)**2 + (a.y - b.y)**2 + (a.z - b.z)**2
    end
  end
end

