# frozen_string_literal: true

require 'matrix'

module PointCloudImporter
  # Performs adaptive sampling of a point cloud for inference guides.
  class InferenceSampler
    DEFAULT_TARGET_COUNT = 50_000
    GRID_CELL_SIZE_METERS = 0.20
    CURVATURE_RADIUS_METERS = 0.10
    MIN_DISTANCE_METERS = 0.05

    def initialize(points, target_count = DEFAULT_TARGET_COUNT, progress_callback: nil)
      @points = points
      @target_count = [target_count.to_i, 1].max
      @progress_callback = progress_callback
      @grid = nil
      @cell_size = convert_length(GRID_CELL_SIZE_METERS)
      @curvature_radius = convert_length(CURVATURE_RADIUS_METERS)
      @min_distance = convert_length(MIN_DISTANCE_METERS)
    end

    def compute_sample
      return [] unless @points && @points.length.positive?

      report_progress(:grid, 0.0)
      build_spatial_grid!
      report_progress(:grid, 0.2)

      curvatures = compute_curvatures
      report_progress(:curvature, 0.5)

      indices = select_points(curvatures)
      report_progress(:selection, 0.8)

      report_progress(:completed, 1.0)

      if block_given?
        yield(:completed, 1.0)
      end

      indices
    end

    private

    def report_progress(stage, progress)
      @progress_callback&.call(stage, progress)
    end

    def build_spatial_grid!
      @grid = Hash.new { |hash, key| hash[key] = [] }
      return unless @points

      @points.length.times do |index|
        point = @points[index]
        coords = point_coordinates(point)
        next unless coords

        key = cell_key_from_coords(coords)
        @grid[key] << index
      end
    end

    def compute_curvatures
      curvatures = Array.new(@points.length, 0.0)
      return curvatures unless @grid

      @points.length.times do |index|
        point = @points[index]
        coords = point_coordinates(point)
        next unless coords

        neighbor_indices = neighbors_within(coords, @curvature_radius)
        next if neighbor_indices.length < 3

        centroid = compute_centroid(neighbor_indices)
        covariance = compute_covariance(neighbor_indices, centroid)
        eigenvalues = covariance.eigensystem.eigenvalues.map { |value| value.real }
        total = eigenvalues.sum
        next if total <= 0.0

        min_value = eigenvalues.min
        planarity = (min_value / total).clamp(0.0, 1.0)
        curvatures[index] = 1.0 - planarity
      end

      curvatures
    end

    def select_points(curvatures)
      sorted_indices = (0...@points.length).sort_by { |index| -curvatures[index].to_f }
      excluded = Array.new(@points.length, false)
      selected = []

      sorted_indices.each do |index|
        next if excluded[index]
        next unless curvatures[index].positive?

        selected << index
        break if selected.length >= @target_count

        mark_neighbors_unavailable(index, excluded)
      end

      selected
    end

    def mark_neighbors_unavailable(index, excluded)
      point = @points[index]
      coords = point_coordinates(point)
      return unless coords

      neighbors_within(coords, @min_distance).each do |neighbor_index|
        excluded[neighbor_index] = true
      end
    end

    def neighbors_within(point_coords, radius)
      candidates = []
      cell = cell_key_from_coords(point_coords)
      radius_sq = radius * radius
      each_neighbor_cell(cell) do |key|
        next unless @grid.key?(key)

        @grid[key].each do |index|
          candidate = @points[index]
          candidate_coords = point_coordinates(candidate)
          next unless candidate_coords

          distance_sq = squared_distance_coords(point_coords, candidate_coords)
          candidates << index if distance_sq <= radius_sq
        end
      end

      candidates
    end

    def compute_centroid(indices)
      sum_x = 0.0
      sum_y = 0.0
      sum_z = 0.0

      count = 0
      indices.each do |index|
        point = @points[index]
        coords = point_coordinates(point)
        next unless coords

        sum_x += coords[0]
        sum_y += coords[1]
        sum_z += coords[2]
        count += 1
      end

      if count.positive?
        Geom::Point3d.new(sum_x / count, sum_y / count, sum_z / count)
      else
        Geom::Point3d.new
      end
    end

    def compute_covariance(indices, centroid)
      xx = xy = xz = yy = yz = zz = 0.0
      count = 0

      indices.each do |index|
        point = @points[index]
        coords = point_coordinates(point)
        next unless coords

        dx = coords[0] - centroid.x
        dy = coords[1] - centroid.y
        dz = coords[2] - centroid.z

        xx += dx * dx
        xy += dx * dy
        xz += dx * dz
        yy += dy * dy
        yz += dy * dz
        zz += dz * dz

        count += 1
      end

      if count.positive?
        inv = 1.0 / count
        xx *= inv
        xy *= inv
        xz *= inv
        yy *= inv
        yz *= inv
        zz *= inv
      end

      Matrix[[xx, xy, xz], [xy, yy, yz], [xz, yz, zz]]
    end

    def each_neighbor_cell(cell)
      cx, cy, cz = cell
      (-1..1).each do |dx|
        (-1..1).each do |dy|
          (-1..1).each do |dz|
            yield([cx + dx, cy + dy, cz + dz])
          end
        end
      end
    end

    def cell_key(point)
      coords = point_coordinates(point)
      return nil unless coords

      cell_key_from_coords(coords)
    end

    def cell_key_from_coords(coords)
      [
        (coords[0] / @cell_size).floor,
        (coords[1] / @cell_size).floor,
        (coords[2] / @cell_size).floor
      ]
    end

    AXIS_INDICES = { x: 0, y: 1, z: 2 }.freeze

    def point_coordinate(point, axis)
      return nil unless point

      if point.respond_to?(axis)
        value = point.public_send(axis)
      elsif point.respond_to?(:[]) && AXIS_INDICES.key?(axis)
        value = point[AXIS_INDICES[axis]]
      end

      return nil if value.nil?

      value.to_f
    rescue StandardError
      nil
    end

    def point_coordinates(point)
      return nil unless point

      if point.respond_to?(:x) && point.respond_to?(:y) && point.respond_to?(:z)
        [point.x.to_f, point.y.to_f, point.z.to_f]
      elsif point.respond_to?(:[])
        x = point[0]
        y = point[1]
        z = point[2]
        return nil if x.nil? || y.nil? || z.nil?

        [x.to_f, y.to_f, z.to_f]
      else
        nil
      end
    rescue StandardError
      nil
    end

    def squared_distance_coords(a_coords, b_coords)
      dx = a_coords[0] - b_coords[0]
      dy = a_coords[1] - b_coords[1]
      dz = a_coords[2] - b_coords[2]
      (dx * dx) + (dy * dy) + (dz * dz)
    end

    def convert_length(meters)
      if 1.respond_to?(:m)
        meters.m
      else
        meters * 39.37007874015748
      end
    end
  end
end
