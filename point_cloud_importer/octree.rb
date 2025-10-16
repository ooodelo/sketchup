# frozen_string_literal: true

module PointCloudImporter
  # Represents a single node in the spatial octree.
  class OctreeNode
    attr_reader :bounding_box, :point_indices, :children, :depth

    def initialize(bounding_box:, point_indices:, depth: 0)
      @bounding_box = bounding_box
      @point_indices = point_indices
      @depth = depth
      @children = Array.new(8)
    end

    def leaf?
      @children.compact.empty?
    end

    def empty?
      @point_indices.empty? && @children.compact.empty?
    end

    def child(index)
      @children[index]
    end

    def set_child(index, node)
      @children[index] = node
    end

    def each_child
      @children.each do |child|
        yield(child) if child
      end
    end

    def clear
      @point_indices.clear
      @children.each do |child|
        child&.clear
      end
      @children.clear
    end
  end

  # Spatial partitioning structure used to accelerate point cloud rendering.
  class Octree
    DEFAULT_MAX_POINTS_PER_NODE = 100_000
    DEFAULT_MAX_DEPTH = 8

    attr_reader :root, :points, :max_points_per_node, :max_depth
    attr_reader :metadata

    def initialize(points, max_points_per_node: DEFAULT_MAX_POINTS_PER_NODE, max_depth: DEFAULT_MAX_DEPTH)
      @points = points
      @max_points_per_node = [max_points_per_node.to_i, 1].max
      @max_depth = [max_depth.to_i, 1].max
      @root = nil
      @metadata = {}
      @last_query_stats = nil
    end

    def build
      clear
      return unless @points

      indices = gather_indices(@points)
      return if indices.empty?

      bounds = compute_bounds(indices)
      return unless bounds

      @root = OctreeNode.new(bounding_box: bounds, point_indices: indices, depth: 0)
      subdivide(@root)
      update_metadata
      @root
    end

    def clear
      @root&.clear
      @root = nil
      @last_query_stats = nil
    end

    def built?
      !@root.nil?
    end

    def query_frustum(frustum)
      return [] unless frustum
      return [] unless built?

      results = []
      @last_query_stats = {
        nodes_visited: 0,
        nodes_culled: 0,
        points_returned: 0
      }

      traverse_with_frustum(@root, frustum, fully_contained: false, results: results)
      @last_query_stats[:points_returned] = results.length
      results
    end

    def last_query_stats
      @last_query_stats || { nodes_visited: 0, nodes_culled: 0, points_returned: 0 }
    end

    def total_nodes
      count_nodes(@root)
    end

    def leaf_count
      count_leaves(@root)
    end

    def average_points_per_leaf
      leaf_points = []
      collect_leaf_point_counts(@root, leaf_points)
      return 0.0 if leaf_points.empty?

      leaf_points.sum.to_f / leaf_points.length
    end

    def max_depth_actual
      compute_max_depth(@root)
    end

    def draw_debug(view, max_depth: nil)
      return unless view
      return unless built?

      color_cache = {}
      traverse_debug(@root) do |node|
        next if max_depth && node.depth > max_depth

        color_cache[node.depth] ||= debug_color_for_depth(node.depth)
        color = color_cache[node.depth]
        draw_bounds(view, node.bounding_box, color)
      end
    end

    private

    def traverse_with_frustum(node, frustum, fully_contained:, results:)
      return unless node

      @last_query_stats[:nodes_visited] += 1

      unless fully_contained
        intersects = frustum.intersects_box(node.bounding_box)
        unless intersects
          @last_query_stats[:nodes_culled] += 1
          return
        end

        fully_contained = frustum.fully_contains_box(node.bounding_box)
      end

      if fully_contained
        collect_all_indices(node, results)
        return
      end

      if node.leaf?
        results.concat(node.point_indices)
        return
      end

      node.each_child do |child|
        traverse_with_frustum(child, frustum, fully_contained: fully_contained, results: results)
      end
    end

    def subdivide(node)
      stack = []
      stack << node if node

      until stack.empty?
        current = stack.pop
        next unless current

        next if current.point_indices.length <= @max_points_per_node
        next if current.depth >= @max_depth

        bounds = current.bounding_box
        center = bounds.center
        buckets = Array.new(8) { [] }

        current.point_indices.each do |index|
          point = point_for_index(index)
          next unless point

          bucket_index = octant_index(point, center)
          buckets[bucket_index] << index
        end

        current.point_indices.clear

        buckets.each_with_index do |bucket, bucket_index|
          next if bucket.empty?

          child_bounds = bounds_for_octant(bounds, center, bucket_index)
          child = OctreeNode.new(bounding_box: child_bounds, point_indices: bucket, depth: current.depth + 1)
          current.set_child(bucket_index, child)
          stack << child
        end
      end
    end

    def gather_indices(points)
      length = points.respond_to?(:length) ? points.length.to_i : 0
      return [] if length <= 0

      indices = []
      length.times do |index|
        point = point_for_index(index)
        indices << index if point
      end
      indices
    end

    def compute_bounds(indices)
      return nil if indices.empty?

      first_point = point_for_index(indices.first)
      return nil unless first_point

      bbox = Geom::BoundingBox.new
      bbox.add(first_point)

      indices[1..-1]&.each do |index|
        point = point_for_index(index)
        bbox.add(point) if point
      end

      bbox
    end

    def bounds_for_octant(bounds, center, octant_index)
      min = bounds.min
      max = bounds.max

      min_x = ((octant_index & 1).zero? ? min.x : center.x)
      max_x = ((octant_index & 1).zero? ? center.x : max.x)
      min_y = ((octant_index & 2).zero? ? min.y : center.y)
      max_y = ((octant_index & 2).zero? ? center.y : max.y)
      min_z = ((octant_index & 4).zero? ? min.z : center.z)
      max_z = ((octant_index & 4).zero? ? center.z : max.z)

      bbox = Geom::BoundingBox.new
      bbox.add(Geom::Point3d.new(min_x, min_y, min_z))
      bbox.add(Geom::Point3d.new(max_x, max_y, max_z))
      bbox
    end

    def octant_index(point, center)
      normalized_point = point3d_from(point)
      return 0 unless normalized_point

      index = 0
      index |= 1 if normalized_point.x >= center.x
      index |= 2 if normalized_point.y >= center.y
      index |= 4 if normalized_point.z >= center.z
      index
    end

    def collect_all_indices(node, results)
      if node.leaf?
        results.concat(node.point_indices)
        return
      end

      node.each_child do |child|
        collect_all_indices(child, results)
      end
    end

    def count_nodes(node)
      return 0 unless node

      1 + node.children.compact.sum { |child| count_nodes(child) }
    end

    def count_leaves(node)
      return 0 unless node
      return 1 if node.leaf?

      node.children.compact.sum { |child| count_leaves(child) }
    end

    def collect_leaf_point_counts(node, accumulator)
      return unless node

      if node.leaf?
        accumulator << node.point_indices.length
        return
      end

      node.children.compact.each do |child|
        collect_leaf_point_counts(child, accumulator)
      end
    end

    def compute_max_depth(node)
      return 0 unless node
      return node.depth if node.leaf?

      node.children.compact.map { |child| compute_max_depth(child) }.max || node.depth
    end

    def traverse_debug(node, &block)
      return unless node

      yield(node)
      node.children.compact.each { |child| traverse_debug(child, &block) }
    end

    def debug_color_for_depth(depth)
      hue = ((depth % 8) / 8.0) * 360.0
      saturation = 0.6
      value = 0.9
      rgb = hsv_to_rgb(hue, saturation, value)
      Sketchup::Color.new(*rgb)
    end

    def hsv_to_rgb(hue, saturation, value)
      c = value * saturation
      x = c * (1 - ((hue / 60.0) % 2 - 1).abs)
      m = value - c

      r = g = b = 0.0

      case hue
      when 0...60
        r, g, b = c, x, 0
      when 60...120
        r, g, b = x, c, 0
      when 120...180
        r, g, b = 0, c, x
      when 180...240
        r, g, b = 0, x, c
      when 240...300
        r, g, b = x, 0, c
      else
        r, g, b = c, 0, x
      end

      [((r + m) * 255).round, ((g + m) * 255).round, ((b + m) * 255).round]
    end

    def draw_bounds(view, bounds, color)
      corners = bounding_box_corners(bounds)
      return if corners.empty?

      edges = [
        [0, 1], [1, 3], [3, 2], [2, 0],
        [4, 5], [5, 7], [7, 6], [6, 4],
        [0, 4], [1, 5], [2, 6], [3, 7]
      ]

      view.drawing_color = color
      points = []
      edges.each do |start_index, end_index|
        points << corners[start_index]
        points << corners[end_index]
      end
      view.draw(GL_LINES, points)
    end

    def bounding_box_corners(bounds)
      return [] unless bounds

      min = bounds.min
      max = bounds.max
      [
        Geom::Point3d.new(min.x, min.y, min.z),
        Geom::Point3d.new(max.x, min.y, min.z),
        Geom::Point3d.new(min.x, max.y, min.z),
        Geom::Point3d.new(max.x, max.y, min.z),
        Geom::Point3d.new(min.x, min.y, max.z),
        Geom::Point3d.new(max.x, min.y, max.z),
        Geom::Point3d.new(min.x, max.y, max.z),
        Geom::Point3d.new(max.x, max.y, max.z)
      ]
    end

    def update_metadata
      @metadata = {
        max_points_per_node: @max_points_per_node,
        max_depth: @max_depth,
        total_nodes: total_nodes,
        leaves: leaf_count,
        max_depth_actual: max_depth_actual
      }
    end
  end

  # Represents camera viewing frustum for SketchUp views.
  class Frustum
    Plane = Struct.new(:point, :normal) do
      def distance_to(candidate)
        vector = candidate - point
        normal.dot(vector)
      end
    end

    attr_reader :planes

    DEFAULT_NEAR_DISTANCE = 0.1
    DEFAULT_FAR_DISTANCE = 10_000.0

    def self.from_view(view)
      return unless view
      return unless view.respond_to?(:camera)

      camera = view.camera
      return unless camera
      return unless camera.respond_to?(:perspective?) && camera.perspective?

      eye = camera.eye
      target = camera.target
      up = camera.up

      forward = target - eye
      return if forward.length.zero?
      forward.normalize!

      up ||= Geom::Vector3d.new(0, 0, 1)
      up = up.clone
      if up.length.zero?
        up = Geom::Vector3d.new(0, 0, 1)
      else
        up.normalize!
      end

      right = forward.cross(up)
      if right.length.zero?
        right = forward.cross(Geom::Vector3d.new(0, 0, 1))
        if right.length.zero?
          right = Geom::Vector3d.new(1, 0, 0)
        end
      end
      right.normalize!

      actual_up = right.cross(forward)
      actual_up.normalize!

      fov = camera.respond_to?(:fov) ? camera.fov.to_f : 0.0
      return if fov <= 0.0
      fov_rad = fov * Math::PI / 180.0

      aspect = aspect_ratio(view)

      near_distance = extract_distance(camera, %i[znear near_clip near], DEFAULT_NEAR_DISTANCE)
      near_distance = DEFAULT_NEAR_DISTANCE if near_distance <= 0.0
      far_distance = extract_distance(camera, %i[zfar far_clip far], DEFAULT_FAR_DISTANCE)
      far_distance = DEFAULT_FAR_DISTANCE if far_distance <= near_distance

      near_center = eye.offset(forward, near_distance)
      far_center = eye.offset(forward, far_distance)

      near_height = Math.tan(fov_rad * 0.5) * near_distance
      near_width = near_height * aspect
      far_height = Math.tan(fov_rad * 0.5) * far_distance
      far_width = far_height * aspect

      up_near = scale_vector(actual_up, near_height)
      right_near = scale_vector(right, near_width)
      up_far = scale_vector(actual_up, far_height)
      right_far = scale_vector(right, far_width)

      ntl = near_center.offset(up_near - right_near)
      ntr = near_center.offset(up_near + right_near)
      nbl = near_center.offset(-up_near - right_near)
      nbr = near_center.offset(-up_near + right_near)

      ftl = far_center.offset(up_far - right_far)
      ftr = far_center.offset(up_far + right_far)
      fbl = far_center.offset(-up_far - right_far)
      fbr = far_center.offset(-up_far + right_far)

      planes = []

      near_normal = forward.clone
      near_normal.normalize!
      planes << Plane.new(near_center, near_normal)

      far_normal = forward.clone
      far_normal.reverse!
      planes << Plane.new(far_center, far_normal)

      planes << side_plane(eye, nbl, ntl, forward)
      planes << side_plane(eye, ntr, nbr, forward)
      planes << side_plane(eye, ntr, ntl, forward)
      planes << side_plane(eye, nbl, nbr, forward)

      new(planes.compact)
    end

    def initialize(planes)
      @planes = planes
    end

    def intersects_box(box)
      return false unless box
      return false if @planes.empty?

      corners = bounding_box_corners(box)
      @planes.each do |plane|
        distances = corners.map { |corner| plane.distance_to(corner) }
        return false if distances.all? { |distance| distance < 0.0 }
      end

      true
    end

    def fully_contains_box(box)
      return false unless box
      return false if @planes.empty?

      corners = bounding_box_corners(box)
      @planes.all? do |plane|
        corners.all? { |corner| plane.distance_to(corner) >= 0.0 }
      end
    end

    private

    def self.aspect_ratio(view)
      width = view.respond_to?(:vpwidth) ? view.vpwidth.to_f : 0.0
      height = view.respond_to?(:vpheight) ? view.vpheight.to_f : 0.0
      return 1.0 if width <= 0.0 || height <= 0.0

      width / height
    end

    def self.extract_distance(camera, candidates, default)
      candidates.each do |name|
        next unless camera.respond_to?(name)

        begin
          value = camera.public_send(name).to_f
          return value if value.positive?
        rescue StandardError
          next
        end
      end

      default
    end

    def self.scale_vector(vector, scalar)
      Geom::Vector3d.new(vector.x * scalar, vector.y * scalar, vector.z * scalar)
    end

    def self.side_plane(eye, point_a, point_b, forward)
      vector_a = point_a - eye
      vector_b = point_b - eye
      normal = vector_a.cross(vector_b)
      return if normal.length.zero?

      normal.normalize!
      normal.reverse! if normal.dot(forward) < 0.0
      Plane.new(eye, normal)
    end

    def bounding_box_corners(box)
      min_point = box.min
      max_point = box.max
      [
        Geom::Point3d.new(min_point.x, min_point.y, min_point.z),
        Geom::Point3d.new(max_point.x, min_point.y, min_point.z),
        Geom::Point3d.new(min_point.x, max_point.y, min_point.z),
        Geom::Point3d.new(max_point.x, max_point.y, min_point.z),
        Geom::Point3d.new(min_point.x, min_point.y, max_point.z),
        Geom::Point3d.new(max_point.x, min_point.y, max_point.z),
        Geom::Point3d.new(min_point.x, max_point.y, max_point.z),
        Geom::Point3d.new(max_point.x, max_point.y, max_point.z)
      ]
    end

    def point_for_index(index)
      return nil unless @points

      raw_point = @points[index]
      converted = point3d_from(raw_point)
      if converted && !converted.equal?(raw_point) && @points.respond_to?(:[]=)
        @points[index] = converted
      end
      converted
    end

    def point3d_from(point)
      return point if point.is_a?(Geom::Point3d)

      if point.respond_to?(:x) && point.respond_to?(:y) && point.respond_to?(:z)
        Geom::Point3d.new(point.x.to_f, point.y.to_f, point.z.to_f)
      elsif point.respond_to?(:[])
        x = point[0]
        y = point[1]
        z = point[2]
        return nil if x.nil? || y.nil? || z.nil?

        Geom::Point3d.new(x.to_f, y.to_f, z.to_f)
      end
    rescue StandardError
      nil
    end

    class << self
      private :aspect_ratio, :extract_distance, :scale_vector, :side_plane
    end
  end
end
