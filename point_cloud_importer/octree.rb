# frozen_string_literal: true

module PointCloudImporter
  # Spatial subdivision of point samples using an octree.
  class Octree
    MAX_POINTS_PER_NODE = 100_000
    MIN_DIAGONAL_LENGTH = 1e-4

    # Simple axis aligned bounding box helper.
    class Bounds
      attr_accessor :min, :max

      def self.from_indices(indices, points)
        raise ArgumentError, 'indices cannot be empty' if indices.empty?

        first_point = nil
        indices.each do |index|
          first_point = points[index]
          break if first_point
        end

        raise ArgumentError, 'no points available for bounds' unless first_point

        min_point = Geom::Point3d.new(first_point.x, first_point.y, first_point.z)
        max_point = Geom::Point3d.new(first_point.x, first_point.y, first_point.z)

        indices.each do |index|
          point = points[index]
          next unless point

          min_point.x = [min_point.x, point.x].min
          min_point.y = [min_point.y, point.y].min
          min_point.z = [min_point.z, point.z].min

          max_point.x = [max_point.x, point.x].max
          max_point.y = [max_point.y, point.y].max
          max_point.z = [max_point.z, point.z].max
        end

        new(min_point, max_point)
      end

      def initialize(min_point, max_point)
        @min = min_point
        @max = max_point
      end

      def center
        Geom::Point3d.new((@min.x + @max.x) * 0.5, (@min.y + @max.y) * 0.5, (@min.z + @max.z) * 0.5)
      end

      def diagonal_length
        dx = @max.x - @min.x
        dy = @max.y - @min.y
        dz = @max.z - @min.z
        Math.sqrt(dx * dx + dy * dy + dz * dz)
      end

      def expand_to_include!(other)
        return self unless other

        @min.x = [@min.x, other.min.x].min
        @min.y = [@min.y, other.min.y].min
        @min.z = [@min.z, other.min.z].min

        @max.x = [@max.x, other.max.x].max
        @max.y = [@max.y, other.max.y].max
        @max.z = [@max.z, other.max.z].max
        self
      end

      def corners
        min_x = @min.x
        min_y = @min.y
        min_z = @min.z
        max_x = @max.x
        max_y = @max.y
        max_z = @max.z

        [
          Geom::Point3d.new(min_x, min_y, min_z),
          Geom::Point3d.new(max_x, min_y, min_z),
          Geom::Point3d.new(min_x, max_y, min_z),
          Geom::Point3d.new(max_x, max_y, min_z),
          Geom::Point3d.new(min_x, min_y, max_z),
          Geom::Point3d.new(max_x, min_y, max_z),
          Geom::Point3d.new(min_x, max_y, max_z),
          Geom::Point3d.new(max_x, max_y, max_z)
        ]
      end

      def copy
        Bounds.new(
          Geom::Point3d.new(@min.x, @min.y, @min.z),
          Geom::Point3d.new(@max.x, @max.y, @max.z)
        )
      end
    end

    # Tree node storing the point indices belonging to the region.
    class Node
      attr_reader :bounds, :indices, :children, :parent
      attr_accessor :dirty

      def initialize(bounds:, indices:, parent: nil)
        @bounds = bounds
        @indices = indices
        @children = []
        @parent = parent
        @dirty = false
      end

      def add_child(child)
        @children << child
      end

      def children?
        !@children.empty?
      end

      def leaf?
        @children.empty?
      end

      def bounds=(new_bounds)
        @bounds = new_bounds
      end
    end

    def initialize(points, max_points_per_node: MAX_POINTS_PER_NODE)
      @points = points
      @max_points_per_node = max_points_per_node
      @index_map = {}
      @root = build_root
    end

    def empty?
      !@root
    end

    def each_leaf_intersecting(frustum, &block)
      return enum_for(:each_leaf_intersecting, frustum) unless block_given?
      return if empty?

      traverse_intersections(@root, frustum, &block)
    end

    def update_indices(indices)
      return if empty?

      dirty = []
      indices.each do |index|
        node = @index_map[index]
        next unless node

        mark_dirty(node, dirty)
      end

      recompute_dirty_bounds(@root) unless dirty.empty?
    end

    def rebuild!
      @index_map.clear
      @root = build_root
    end

    private

    def build_root
      return nil if @points.nil? || @points.empty?

      indices = (0...@points.length).to_a
      bounds = Bounds.from_indices(indices, @points)
      build_node(indices, bounds, nil)
    end

    def build_node(indices, bounds, parent)
      node = Node.new(bounds: bounds, indices: indices, parent: parent)

      if indices.length <= @max_points_per_node || bounds.diagonal_length <= MIN_DIAGONAL_LENGTH
        register_leaf(node)
        return node
      end

      center = bounds.center
      buckets = Array.new(8) { [] }
      indices.each do |index|
        point = @points[index]
        next unless point

        buckets[octant_index(point, center)] << index
      end

      non_empty = buckets.reject(&:empty?)
      if non_empty.length <= 1
        register_leaf(node)
        return node
      end

      node.indices.clear
      buckets.each do |bucket|
        next if bucket.empty?

        child_bounds = Bounds.from_indices(bucket, @points)
        child = build_node(bucket, child_bounds, node)
        node.add_child(child)
      end

      node
    end

    def register_leaf(node)
      node.indices.each { |index| @index_map[index] = node }
    end

    def traverse_intersections(node, frustum, &block)
      return unless frustum.intersects_bounds?(node.bounds)

      if node.leaf?
        yield(node)
        return
      end

      node.children.each { |child| traverse_intersections(child, frustum, &block) }
    end

    def mark_dirty(node, dirty)
      current = node
      while current && !current.dirty
        current.dirty = true
        dirty << current
        current = current.parent
      end
    end

    def recompute_dirty_bounds(node)
      return unless node.dirty

      if node.leaf?
        node.bounds = Bounds.from_indices(node.indices, @points)
        node.dirty = false
        return
      end

      node.children.each { |child| recompute_dirty_bounds(child) if child.dirty }

      combined = node.children.first.bounds.copy
      node.children.each do |child|
        next if child == node.children.first

        combined.expand_to_include!(child.bounds)
      end

      node.bounds = combined
      node.dirty = false
    end

    def octant_index(point, center)
      index = 0
      index |= 1 if point.x >= center.x
      index |= 2 if point.y >= center.y
      index |= 4 if point.z >= center.z
      index
    end
  end
end
