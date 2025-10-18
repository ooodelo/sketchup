# encoding: utf-8
# frozen_string_literal: true

require 'matrix'

require_relative 'spatial_index'
require_relative 'numbers'

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

      normalized_points, index_mapping = normalized_points_and_indices
      return [] if normalized_points.empty?

      report_progress(:grid, 0.1)

      spatial_index = SpatialIndex.new(normalized_points, index_mapping, cache: false)

      center, bounding_radius = bounds_statistics(normalized_points)
      candidates = candidate_order(normalized_points, index_mapping, center)

      report_progress(:curvature, 0.5)

      selected = []
      excluded = {}
      total_candidates = candidates.length.nonzero? || 1

      candidates.each_with_index do |candidate, position|
        original_index = candidate[:index]
        next if excluded[original_index]

        point = candidate[:point]
        radius = adaptive_poisson_radius(candidate[:distance], bounding_radius)
        neighbors = spatial_index.within_radius(point, radius)
        neighbors.each { |neighbor| excluded[neighbor[:index]] = true }

        selected << original_index
        excluded[original_index] = true

        progress_fraction = (position + 1).to_f / total_candidates
        report_sampling_progress(progress_fraction)

        break if selected.length >= @target_count
      end

      report_progress(:completed, 1.0)

      if block_given?
        yield(:completed, 1.0)
      end

      selected
    end

    private

    def normalized_points_and_indices
      return [[], []] unless @points

      points = []
      indices = []
      @points.length.times do |index|
        coords = point_coordinates(@points[index])
        next unless coords

        points << Geom::Point3d.new(coords[0], coords[1], coords[2])
        indices << index
      end

      [points, indices]
    end

    def bounds_statistics(points)
      return [Geom::Point3d.new, 0.0] if points.empty?

      min_x = points.first.x
      max_x = points.first.x
      min_y = points.first.y
      max_y = points.first.y
      min_z = points.first.z
      max_z = points.first.z

      points.each do |point|
        min_x = [min_x, point.x].min
        max_x = [max_x, point.x].max
        min_y = [min_y, point.y].min
        max_y = [max_y, point.y].max
        min_z = [min_z, point.z].min
        max_z = [max_z, point.z].max
      end

      center = Geom::Point3d.new((min_x + max_x) * 0.5, (min_y + max_y) * 0.5, (min_z + max_z) * 0.5)
      bounding_radius = points.map { |point| center.distance(point) }.max || 0.0
      [center, bounding_radius]
    end

    def candidate_order(points, indices, center)
      points.each_with_index.map do |point, local_index|
        {
          index: indices[local_index],
          point: point,
          distance: center.distance(point)
        }
      end.sort_by { |entry| -entry[:distance] }
    end

    def adaptive_poisson_radius(distance_from_center, bounding_radius)
      base = if bounding_radius.positive?
               bounding_radius / Math.sqrt(@target_count.to_f + 1.0)
             else
               @min_distance
             end

      base = [base, @min_distance].max
      falloff = if bounding_radius.positive?
                  (distance_from_center / bounding_radius).clamp(0.0, 1.0)
                else
                  0.0
                end

      scale = 1.0 + (falloff * 0.5)
      base * scale
    end

    def report_sampling_progress(fraction)
      eased = Numbers.clamp(fraction.to_f, 0.0, 1.0)
      stage_progress = 0.3 + (0.5 * eased)
      report_progress(:selection, stage_progress)
    end

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
