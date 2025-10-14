# frozen_string_literal: true

module PointCloudImporter
  # A lightweight KD-tree for nearest neighbor lookup.
  class SpatialIndex
    Node = Struct.new(:point, :index, :axis, :left, :right)

    def initialize(points, indices)
      @root = build(points.each_with_index.map { |point, local_index| [point, indices[local_index]] })
    end

    def nearest(target)
      return unless @root

      best = nil
      search(@root, target, best)
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

    def search(node, target, best)
      return best unless node

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
