# frozen_string_literal: true

module PointCloudImporter
  # A lightweight KD-tree for nearest neighbor lookup.
  class SpatialIndex
    Node = Struct.new(:point, :index, :axis, :left, :right)

    def initialize(points, indices)
      @root = build(points.each_with_index.map { |point, local_index| [point, indices[local_index]] })
    end

    def nearest(target, max_distance: nil)
      return unless @root

      best = nil
      max_distance_sq = max_distance ? max_distance**2 : nil
      search(@root, target, best, max_distance_sq)
    end

    private

    def build(data, depth = 0)
      return nil if data.empty?

      axis = depth % 3
      data.sort_by! { |entry| coordinate(entry[0], axis) }
      median = data.length / 2

      node = Node.new
      node.point = data[median][0]
      node.index = data[median][1]
      node.axis = axis

      left_data = data[0...median]
      right_data = data[(median + 1)..]

      node.left = build(left_data, depth + 1) unless left_data.empty?
      node.right = build(right_data, depth + 1) unless right_data.nil? || right_data.empty?
      node
    end

    def search(node, target, best, max_distance_sq)
      return best unless node

      axis = node.axis
      value = coordinate(target, axis)
      node_value = coordinate(node.point, axis)

      next_branch = value < node_value ? node.left : node.right
      opposite_branch = value < node_value ? node.right : node.left

      best = search(next_branch, target, best, max_distance_sq)

      current_distance = squared_distance(node.point, target)
      within_limit = max_distance_sq.nil? || current_distance <= max_distance_sq
      if within_limit && (best.nil? || current_distance < best[:distance])
        best = { point: node.point, index: node.index, distance: current_distance }
      end

      axis_distance = (value - node_value)**2
      threshold = if best
                    best[:distance]
                  else
                    max_distance_sq || Float::INFINITY
                  end
      threshold = [threshold, max_distance_sq].compact.min

      if opposite_branch && axis_distance <= threshold
        best = search(opposite_branch, target, best, max_distance_sq)
      end

      best
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
