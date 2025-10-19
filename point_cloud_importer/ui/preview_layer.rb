# encoding: utf-8
# frozen_string_literal: true

require 'weakref'

module PointCloudImporter
  module UI
    # Draws a lightweight preview of point clouds while render caches are empty.
    module PreviewLayer
      PREVIEW_POINT_LIMIT = 2_000
      PREVIEW_SCAN_LIMIT = 50_000
      PREVIEW_POINT_SIZE = 2
      PREVIEW_COLOR = Sketchup::Color.new(0, 0, 0)

      module_function

      def draw(view, tool)
        return unless view
        return unless tool

        clouds = extract_clouds(tool)
        return if clouds.empty?

        samples = collect_samples(clouds)
        return if samples.empty?

        draw_points(view, samples)
      end

      def collect_samples(clouds)
        samples = []

        clouds.each do |cloud|
          next unless preview_candidate?(cloud)

          sample = preview_points_for(cloud)
          next if sample.nil? || sample.empty?

          samples.concat(sample)
        end

        samples
      end
      private_class_method :collect_samples

      def preview_candidate?(cloud)
        return false unless cloud
        return false unless cloud.respond_to?(:visible?) && cloud.visible?
        return false unless cloud.respond_to?(:render_cache_preparation_pending?)
        return false unless cloud.render_cache_preparation_pending?

        points_available?(cloud)
      end
      private_class_method :preview_candidate?

      def points_available?(cloud)
        return false unless cloud.respond_to?(:points)

        points = cloud.points
        points && points.respond_to?(:length) && points.length.positive?
      end
      private_class_method :points_available?

      def extract_clouds(tool)
        if tool.respond_to?(:clouds)
          Array(tool.clouds)
        elsif tool.respond_to?(:active_cloud)
          [tool.active_cloud].compact
        elsif tool.is_a?(Array)
          tool.compact
        elsif tool.respond_to?(:each)
          tool.to_a.compact
        else
          []
        end
      rescue StandardError
        []
      end
      private_class_method :extract_clouds

      def draw_points(view, points)
        style = PointCloud.style_constant(:round) if defined?(PointCloud)
        options = { size: PREVIEW_POINT_SIZE, color: PREVIEW_COLOR }
        options[:style] = style if style

        view.line_width = 0 if view.respond_to?(:line_width=)
        view.draw_points(points, **options)
      rescue StandardError
        nil
      end
      private_class_method :draw_points

      def preview_points_for(cloud)
        points = cloud.points
        return [] unless points

        total = safe_length(points)
        return [] if total.zero?

        cache = sample_cache_for(cloud, total)
        return cache if cache

        sampled = build_sample(points, total)
        store_sample(cloud, sampled, total)
        sampled
      end
      private_class_method :preview_points_for

      def build_sample(points, total)
        limit = [PREVIEW_POINT_LIMIT, total].min
        scan_limit = [PREVIEW_SCAN_LIMIT, total].min
        step = [(scan_limit.to_f / limit).ceil, 1].max

        sample = []
        index = 0
        enumerator = points.respond_to?(:each_with_yield) ? :each_with_yield : :each

        points.public_send(enumerator) do |raw_point|
          break if index >= scan_limit

          if (index % step).zero?
            point3d = coerce_point(raw_point)
            sample << point3d if point3d
            break if sample.length >= limit
          end

          index += 1
        end

        sample
      rescue StandardError
        []
      end
      private_class_method :build_sample

      def coerce_point(raw_point)
        return raw_point if raw_point.is_a?(Geom::Point3d)

        if raw_point.respond_to?(:x) && raw_point.respond_to?(:y) && raw_point.respond_to?(:z)
          Geom::Point3d.new(raw_point.x.to_f, raw_point.y.to_f, raw_point.z.to_f)
        elsif raw_point.respond_to?(:[])
          x = raw_point[0]
          y = raw_point[1]
          z = raw_point[2]
          return nil if x.nil? || y.nil? || z.nil?

          Geom::Point3d.new(x.to_f, y.to_f, z.to_f)
        end
      rescue StandardError
        nil
      end
      private_class_method :coerce_point

      def safe_length(collection)
        collection.length.to_i
      rescue StandardError
        0
      end
      private_class_method :safe_length

      def sample_cache
        @sample_cache ||= {}
      end
      private_class_method :sample_cache

      def sample_cache_for(cloud, total_length)
        key = cloud.__id__
        cache = sample_cache[key]
        return unless cache

        reference = cache[:reference]
        begin
          reference.__getobj__
        rescue WeakRef::RefError
          sample_cache.delete(key)
          return
        end

        cached_points = cache[:points]
        return unless cached_points && !cached_points.empty?

        cached_length = cache[:length].to_i
        desired_count = [PREVIEW_POINT_LIMIT, total_length].min

        if cached_length == total_length && cached_points.length >= desired_count
          return cached_points
        end

        if cached_length < total_length && cached_points.length >= PREVIEW_POINT_LIMIT
          return cached_points
        end

        sample_cache.delete(key)
        nil
      end
      private_class_method :sample_cache_for

      def store_sample(cloud, points, total_length)
        key = cloud.__id__
        sample_cache[key] = { reference: WeakRef.new(cloud), points: points, length: total_length }
      rescue StandardError
        nil
      end
      private_class_method :store_sample
    end
  end
end
