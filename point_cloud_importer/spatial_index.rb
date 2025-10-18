# encoding: utf-8
# frozen_string_literal: true

require 'json'
require 'digest/sha1'
require 'fileutils'
require 'tmpdir'

require_relative 'clock'
require_relative 'logger'
require_relative 'numbers'

module PointCloudImporter
  # A lightweight KD-tree for nearest neighbor lookup.
  class SpatialIndex
    DEFAULT_MAX_DEPTH = 50
    SAH_RATIO_THRESHOLD = 4.0
    SAH_MIN_SIZE = 32
    MAX_SAH_BINS = 16
    CACHE_DIR = File.join(Dir.tmpdir, 'point_cloud_importer')

    class Node
      attr_accessor :point, :index, :axis, :left, :right, :entries

      def initialize(point: nil, index: nil, axis: nil, left: nil, right: nil, entries: nil)
        @point = point
        @index = index
        @axis = axis
        @left = left
        @right = right
        @entries = entries
      end

      def leaf?
        !@entries.nil?
      end
    end

    def initialize(points, indices, max_depth: DEFAULT_MAX_DEPTH, cache: true)
      @max_depth = max_depth
      @cache_key = cache ? compute_cache_key(points, indices, max_depth) : nil
      @root = load_from_cache(@cache_key) if @cache_key

      unless @root
        builder = build_incremental(points, indices, max_depth: max_depth)
        until builder.finished?
          builder.step(time_budget: nil)
        end
        @root = builder.root
        persist_cache(@cache_key, @root) if @cache_key && @root
      end
    end

    def nearest(target, max_distance: nil)
      return unless @root

      best = nil
      max_distance_sq = max_distance ? max_distance**2 : nil
      search(@root, target, best, max_distance_sq)
    end

    def within_radius(target, radius)
      return [] unless @root

      results = []
      radius_sq = radius.to_f**2
      collect_within_radius(@root, target, radius_sq, results)
      results
    end

    private

    def build_incremental(points, indices, max_depth: DEFAULT_MAX_DEPTH, progress_callback: nil)
      data = gather_entries(points, indices)
      return NullBuilder.new(self) if data.empty?

      Builder.new(self,
                  data: data,
                  max_depth: max_depth,
                  progress_callback: progress_callback)
    end

    def partition_entries(entries, bounds, depth)
      return [nil, nil, nil, nil, nil] if entries.empty?

      sah_axis, sah_split_value = sah_split(entries, bounds)
      axis = sah_axis || (depth % 3)

      if sah_split_value
        pivot_entry = find_pivot(entries, axis, sah_split_value)
        split_value = coordinate(pivot_entry[0], axis) if pivot_entry
      else
        pivot_index = entries.length / 2
        pivot_entry = quickselect(entries, pivot_index, axis)
        split_value = coordinate(pivot_entry[0], axis) if pivot_entry
      end

      return [nil, nil, nil, nil, nil] unless pivot_entry

      left_entries = []
      right_entries = []
      entries.each do |entry|
        next if entry.equal?(pivot_entry)

        coord = coordinate(entry[0], axis)
        if coord < split_value
          left_entries << entry
        else
          right_entries << entry
        end
      end

      if left_entries.empty? && !right_entries.empty?
        left_entries << right_entries.shift
      elsif right_entries.empty? && !left_entries.empty?
        right_entries << left_entries.pop
      end

      if left_entries.empty? && right_entries.empty?
        return [nil, nil, nil, nil, nil]
      end

      [axis, split_value, pivot_entry, left_entries, right_entries]
    end

    def search(node, target, best, max_distance_sq)
      return best unless node

      if node.leaf?
        node.entries.each do |point, index|
          candidate_distance = squared_distance(point, target)
          next if max_distance_sq && candidate_distance > max_distance_sq

          if best.nil? || candidate_distance < best[:distance]
            best = { point: point, index: index, distance: candidate_distance }
          end
        end
        return best
      end

      axis = node.axis
      value = coordinate(target, axis)
      node_value = coordinate(node.point, axis)

      next_branch = value < node_value ? node.left : node.right
      opposite_branch = value < node_value ? node.right : node.left

      best = search(next_branch, target, best, max_distance_sq)

      if best.nil?
        fallback = nearest_from_node(node, target, max_distance_sq)
        return fallback if fallback
      end

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

    def collect_within_radius(node, target, radius_sq, results)
      return unless node

      if node.point
        distance_sq = squared_distance(node.point, target)
        if distance_sq <= radius_sq
          results << { point: node.point, index: node.index, distance: Math.sqrt(distance_sq) }
        end
      end

      if node.leaf?
        node.entries.each do |point, index|
          distance_sq = squared_distance(point, target)
          if distance_sq <= radius_sq
            results << { point: point, index: index, distance: Math.sqrt(distance_sq) }
          end
        end
        return
      end

      axis = node.axis
      value = coordinate(target, axis)
      node_value = coordinate(node.point, axis)

      first_branch = value < node_value ? node.left : node.right
      second_branch = value < node_value ? node.right : node.left

      collect_within_radius(first_branch, target, radius_sq, results)

      axis_distance_sq = (value - node_value)**2
      if second_branch && axis_distance_sq <= radius_sq
        collect_within_radius(second_branch, target, radius_sq, results)
      end
    end

    def nearest_from_node(node, target, max_distance_sq)
      candidates = []

      if node.point
        distance = squared_distance(node.point, target)
        candidates << { point: node.point, index: node.index, distance: distance }
      end

      if node.leaf?
        node.entries.each do |point, index|
          distance = squared_distance(point, target)
          candidates << { point: point, index: index, distance: distance }
        end
      end

      candidates.select! { |candidate| max_distance_sq.nil? || candidate[:distance] <= max_distance_sq }
      candidates.min_by { |candidate| candidate[:distance] }
    end

    def gather_entries(points, indices)
      data = []
      points.each_with_index do |point, local_index|
        entry_index = indices[local_index]
        next if entry_index.nil?

        converted = ensure_point3d(point)
        next unless converted

        data << [converted, entry_index]
      end
      data
    end

    def compute_bounds(entries)
      min_x = Float::INFINITY
      min_y = Float::INFINITY
      min_z = Float::INFINITY
      max_x = -Float::INFINITY
      max_y = -Float::INFINITY
      max_z = -Float::INFINITY

      entries.each do |point, _index|
        min_x = [min_x, point.x].min
        min_y = [min_y, point.y].min
        min_z = [min_z, point.z].min
        max_x = [max_x, point.x].max
        max_y = [max_y, point.y].max
        max_z = [max_z, point.z].max
      end

      {
        min: [min_x, min_y, min_z],
        max: [max_x, max_y, max_z]
      }
    end

    def sah_split(entries, bounds)
      return [nil, nil] if entries.length < SAH_MIN_SIZE || bounds.nil?

      extents = (0..2).map { |axis| bounds[:max][axis] - bounds[:min][axis] }
      max_extent = extents.max
      min_extent = extents.reject { |extent| extent <= 1e-9 }.min

      return [nil, nil] if max_extent <= 1e-9
      return [nil, nil] if min_extent && (max_extent / min_extent) < SAH_RATIO_THRESHOLD

      best_cost = Float::INFINITY
      best_axis = nil
      best_value = nil

      (0..2).each do |axis|
        axis_extent = extents[axis]
        next if axis_extent <= 1e-9

        min_value = bounds[:min][axis]
        max_value = bounds[:max][axis]
        bin_count = Numbers.clamp(Math.log2(entries.length).ceil, 8, MAX_SAH_BINS)
        bin_count = 2 if bin_count < 2

        bins = Array.new(bin_count) { { count: 0, bounds: nil } }
        scale = (bin_count - 1) / axis_extent

        entries.each do |point, _index|
          coordinate_value = coordinate(point, axis)
          bin_index = ((coordinate_value - min_value) * scale).floor
          bin_index = Numbers.clamp(bin_index, 0, bin_count - 1)
          bin = bins[bin_index]
          bin[:count] += 1
          bin[:bounds] = merge_bounds(bin[:bounds], point_bounds(point))
        end

        prefix_counts = Array.new(bin_count, 0)
        prefix_bounds = Array.new(bin_count)
        count = 0
        bounds_acc = nil

        bins.each_with_index do |bin, index|
          count += bin[:count]
          bounds_acc = merge_bounds(bounds_acc, bin[:bounds]) if bin[:bounds]
          prefix_counts[index] = count
          prefix_bounds[index] = bounds_acc
        end

        suffix_counts = Array.new(bin_count, 0)
        suffix_bounds = Array.new(bin_count)
        count = 0
        bounds_acc = nil

        (bin_count - 1).downto(0) do |index|
          bin = bins[index]
          count += bin[:count]
          bounds_acc = merge_bounds(bounds_acc, bin[:bounds]) if bin[:bounds]
          suffix_counts[index] = count
          suffix_bounds[index] = bounds_acc
        end

        parent_area = surface_area(bounds)
        next if parent_area <= 0.0

        (0...(bin_count - 1)).each do |index|
          left_count = prefix_counts[index]
          right_count = suffix_counts[index + 1]
          next if left_count.zero? || right_count.zero?

          left_bounds = prefix_bounds[index]
          right_bounds = suffix_bounds[index + 1]
          next unless left_bounds && right_bounds

          left_area = surface_area(left_bounds)
          right_area = surface_area(right_bounds)

          cost = sah_cost(left_area, left_count, parent_area) + sah_cost(right_area, right_count, parent_area)

          next unless cost < best_cost

          split_value = min_value + ((index + 1).to_f / bin_count) * axis_extent
          best_cost = cost
          best_axis = axis
          best_value = split_value
        end
      end

      [best_axis, best_value]
    end

    def sah_cost(area, count, parent_area)
      return Float::INFINITY if area <= 0.0 || parent_area <= 0.0

      (area / parent_area) * count
    end

    def surface_area(bounds)
      x = bounds[:max][0] - bounds[:min][0]
      y = bounds[:max][1] - bounds[:min][1]
      z = bounds[:max][2] - bounds[:min][2]

      x = 0.0 if x.nan?
      y = 0.0 if y.nan?
      z = 0.0 if z.nan?

      epsilon = 1e-9
      a = [x, epsilon].max
      b = [y, epsilon].max
      c = [z, epsilon].max

      2.0 * (a * b + b * c + c * a)
    end

    def merge_bounds(a, b)
      return b unless a
      return a unless b

      {
        min: [
          [a[:min][0], b[:min][0]].min,
          [a[:min][1], b[:min][1]].min,
          [a[:min][2], b[:min][2]].min
        ],
        max: [
          [a[:max][0], b[:max][0]].max,
          [a[:max][1], b[:max][1]].max,
          [a[:max][2], b[:max][2]].max
        ]
      }
    end

    def point_bounds(point)
      coords = [point.x, point.y, point.z]
      { min: coords.dup, max: coords.dup }
    end

    def find_pivot(entries, axis, value)
      entries.min_by do |entry|
        (coordinate(entry[0], axis) - value).abs
      end
    end

    def quickselect(entries, k, axis)
      left = 0
      right = entries.length - 1

      while left <= right
        pivot_index = partition(entries, left, right, axis)
        return entries[k] if k == pivot_index

        if k < pivot_index
          right = pivot_index - 1
        else
          left = pivot_index + 1
        end
      end

      nil
    end

    def partition(entries, left, right, axis)
      pivot_index = (left + right) / 2
      pivot_value = coordinate(entries[pivot_index][0], axis)
      entries[pivot_index], entries[right] = entries[right], entries[pivot_index]
      store_index = left

      left.upto(right - 1) do |index|
        if coordinate(entries[index][0], axis) < pivot_value
          entries[store_index], entries[index] = entries[index], entries[store_index]
          store_index += 1
        end
      end

      entries[right], entries[store_index] = entries[store_index], entries[right]
      store_index
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

    def ensure_point3d(point)
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

    def compute_cache_key(points, indices, max_depth)
      digest = Digest::SHA1.new
      digest.update("depth:#{max_depth}:")
      points.each_with_index do |point, local_index|
        coords = [point.x, point.y, point.z]
        digest.update(coords.join(':'))
        digest.update(":#{indices[local_index]}")
      end
      digest.hexdigest
    end

    def cache_path(key)
      FileUtils.mkdir_p(CACHE_DIR)
      File.join(CACHE_DIR, "spatial_index_#{key}.json")
    end

    def persist_cache(key, root)
      return unless key && root

      payload = {
        max_depth: @max_depth,
        tree: serialize_node(root)
      }

      File.write(cache_path(key), JSON.generate(payload))
    rescue StandardError
      nil
    end

    def load_from_cache(key)
      return unless key

      path = cache_path(key)
      return unless File.file?(path)

      payload = JSON.parse(File.read(path))
      return nil unless payload['max_depth'].to_i == @max_depth

      deserialize_node(payload['tree']) if payload['tree']
    rescue StandardError
      nil
    end

    def serialize_node(node)
      return nil unless node

      {
        point: node.point ? [node.point.x, node.point.y, node.point.z] : nil,
        index: node.index,
        axis: node.axis,
        entries: node.entries&.map { |point, index| [[point.x, point.y, point.z], index] },
        left: serialize_node(node.left),
        right: serialize_node(node.right)
      }
    end

    def deserialize_node(payload)
      return nil unless payload

      node = Node.new
      if payload['point']
        coords = payload['point']
        node.point = Geom::Point3d.new(coords[0], coords[1], coords[2])
      end
      node.index = payload['index']
      node.axis = payload['axis']

      if payload['entries']
        node.entries = payload['entries'].map do |coords, index|
          [Geom::Point3d.new(coords[0], coords[1], coords[2]), index]
        end
      end

      node.left = deserialize_node(payload['left']) if payload['left']
      node.right = deserialize_node(payload['right']) if payload['right']
      node
    end

    class NullBuilder
      Step = Struct.new(:ready_nodes, :progress, :finished)

      def initialize(_index); end

      def step(time_budget: nil)
        Step.new([], 1.0, true)
      end

      def finished?
        true
      end

      def root
        nil
      end
    end

    class Builder
      Step = Struct.new(:ready_nodes, :progress, :finished)

      def initialize(index, data:, max_depth:, progress_callback: nil)
        @index = index
        @max_depth = max_depth
        @progress_callback = progress_callback
        @queue = []
        @processed = 0
        @total = data.length
        @root = Node.new
        enqueue(@root, data, 0)
        @finished = data.empty?
      end

      def step(time_budget: 0.008)
        return Step.new([], 1.0, true) if finished?

        ready_nodes = []
        start_time = Clock.monotonic
        deadline = time_budget ? start_time + time_budget.to_f : nil

        while (work = @queue.shift)
          node, entries, depth, bounds = work
          process(node, entries, depth, bounds, ready_nodes)
          break if deadline && Clock.monotonic >= deadline
        end

        progress = progress_fraction
        notify(progress, ready_nodes)
        Step.new(ready_nodes, progress, finished?)
      end

      def finished?
        @finished
      end

      def root
        @root
      end

      private

      def enqueue(node, entries, depth)
        bounds = @index.send(:compute_bounds, entries)
        @queue << [node, entries, depth, bounds]
      end

      def process(node, entries, depth, bounds, ready_nodes)
        if depth >= @max_depth || entries.length <= 1
          node.entries = entries
          ready_nodes << node
          @processed += entries.length
          mark_finished_if_idle
          return
        end

        axis, _split_value, pivot_entry, left_entries, right_entries =
          @index.send(:partition_entries, entries, bounds, depth)

        if pivot_entry.nil? || axis.nil?
          node.entries = entries
          ready_nodes << node
          @processed += entries.length
          mark_finished_if_idle
          return
        end

        node.axis = axis
        node.point = pivot_entry[0]
        node.index = pivot_entry[1]
        @processed += 1

        unless left_entries.empty?
          node.left = Node.new
          enqueue(node.left, left_entries, depth + 1)
        end

        unless right_entries.empty?
          node.right = Node.new
          enqueue(node.right, right_entries, depth + 1)
        end

        mark_finished_if_idle
      end

      def mark_finished_if_idle
        @finished = @queue.empty?
      end

      def progress_fraction
        return 1.0 if @total.zero?

        value = @processed.to_f / @total
        Numbers.clamp(value, 0.0, 1.0)
      rescue StandardError
        0.0
      end

      def notify(progress, ready_nodes)
        return unless @progress_callback

        @progress_callback.call(progress, ready_nodes.compact)
      rescue StandardError => e
        Logger.debug { "Spatial index builder progress failed: #{e.class}: #{e.message}" }
      end

    end
  end
end
