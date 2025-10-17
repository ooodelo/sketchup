# encoding: utf-8
# frozen_string_literal: true

require 'singleton'
require 'thread'

require_relative 'point_cloud'
require_relative 'logger'
require_relative 'viewer_overlay'
require_relative 'measure_tool'
require_relative 'measurement_history'
require_relative 'main_thread_queue'
require_relative 'threading'

module PointCloudImporter
  # Central registry for loaded point clouds.
  class Manager
    include Singleton

    def initialize
      @clouds_lock = Mutex.new
      @clouds = []
      @overlay = nil
      @active_cloud = nil
      @measurement_history = MeasurementHistory.new(self)
      @cloud_registry = {}
    end

    def clouds
      with_clouds_lock { @clouds.dup }
    end

    def active_cloud
      with_clouds_lock { @active_cloud }
    end

    def active_cloud=(cloud)
      dispatch_to_main_thread { set_active_cloud!(cloud) }
    end

    def add_cloud(cloud)
      dispatch_to_main_thread { register_cloud!(cloud) }
    end

    def remove_cloud(cloud)
      dispatch_to_main_thread { unregister_cloud!(cloud) }
    end

    def clear!
      Threading.guard(:ui, message: 'Manager#clear!')
      clouds_to_dispose = []
      overlay_to_remove = nil
      model = Sketchup.active_model
      supports_overlays = model.respond_to?(:model_overlays)

      with_clouds_lock do
        clouds_to_dispose = @clouds.dup
        @clouds.clear
        @cloud_registry.clear
        @active_cloud = nil
        if supports_overlays
          overlay_to_remove = @overlay
          @overlay = nil
        end
      end

      clouds_to_dispose.each do |cloud|
        cloud.manager = nil if cloud.respond_to?(:manager=)
        cloud.dispose!
        warn_unless_disposed(cloud)
      end

      Logger.debug do
        disposed_names = clouds_to_dispose.map(&:name)
        "Все облака удалены. Количество: #{clouds_to_dispose.length}. Список: #{disposed_names.inspect}"
      end

      if overlay_to_remove
        model.model_overlays.remove(overlay_to_remove)
      end

      refresh_ui_panel
    end

    def ensure_overlay!
      Threading.guard(:ui, message: 'Manager#ensure_overlay!')
      return unless Sketchup.active_model.respond_to?(:model_overlays)
      return if @overlay

      @overlay = ViewerOverlay.new(self)
      Sketchup.active_model.model_overlays.add(@overlay)
    end

    def toggle_active_visibility
      Threading.guard(:ui, message: 'Manager#toggle_active_visibility')
      cloud = active_cloud
      return unless cloud

      cloud.visible = !cloud.visible?
      cloud.sync_inference_visibility!
      view.invalidate if view
      refresh_ui_panel
    end

    def toggle_active_inference_guides
      Threading.guard(:ui, message: 'Manager#toggle_active_inference_guides')
      cloud = active_cloud
      model = Sketchup.active_model
      return unless cloud && model

      cloud.toggle_inference_guides!(model)
      view.invalidate if view
      refresh_ui_panel
    end

    def view
      Sketchup.active_model&.active_view
    end

    def draw(view)
      Threading.guard(:ui, message: 'Manager#draw')
      clouds_snapshot = clouds

      clouds_snapshot.each do |cloud|
        next unless cloud.visible?

        cloud.draw(view)
      end

      draw_preview_measurement(view)
    end

    def pick_point(view, x, y)
      cloud = active_cloud
      return unless cloud&.visible?
      return unless cloud.points && cloud.points.length.positive?

      ray = view.pickray(x, y)
      return unless ray

      cloud.closest_point_to_ray(ray, view: view, pixel_tolerance: 10)
    end

    def measurement_tool
      MeasureTool.new(self)
    end

    def measurement_history
      @measurement_history
    end

    def export_active_cloud
      cloud = active_cloud
      unless cloud
        ::UI.messagebox('Нет активного облака для экспорта.')
        return
      end

      default_name = cloud.name.to_s.strip
      default_name = 'point_cloud' if default_name.empty?
      default_name = default_name.gsub(/[^\w\-.]+/, '_')
      default_name << '.ply' unless default_name.downcase.end_with?('.ply')

      path = ::UI.savepanel('Экспорт облака точек', nil, default_name)
      return unless path

      begin
        cloud.export_to_ply(path)
        ::UI.messagebox("Облако успешно экспортировано в #{File.basename(path)}.")
      rescue StandardError => e
        ::UI.messagebox("Не удалось экспортировать облако: #{e.message}")
      end
    end

    def save_measurement_as_dimension(entry)
      return unless entry

      model = Sketchup.active_model
      return unless model

      from = entry[:from]
      to = entry[:to]
      return unless from && to

      vector = to - from
      return if vector.length.zero?

      offset = vector.cross(Geom::Vector3d.new(0, 0, 1))
      offset = vector.cross(Geom::Vector3d.new(0, 1, 0)) if offset.length.zero?
      offset = vector.cross(Geom::Vector3d.new(1, 0, 0)) if offset.length.zero?
      return if offset.length.zero?

      offset.normalize!
      offset_length = [vector.length * 0.25, 0.1.m].max
      offset *= offset_length

      point_on_dim = to.offset(offset)

      entities = model.active_entities
      model.start_operation('Create Measurement Dimension', true)
      dimension = entities.add_dimension_linear(from, to, point_on_dim)
      dimension.text = entry[:label] if dimension.respond_to?(:text=) && entry[:label]
      model.commit_operation
      dimension
    rescue StandardError => e
      model.abort_operation if model && model.respond_to?(:abort_operation)
      warn("[PointCloudImporter] Failed to create dimension: #{e.message}")
      nil
    ensure
      view&.invalidate
    end

    private

    def dispatch_to_main_thread(&block)
      return unless block

      MainThreadDispatcher.enqueue(&block)
    end

    def register_cloud!(cloud)
      Threading.guard(:ui, message: 'Manager#register_cloud!')
      return unless cloud

      point_count = cloud.points ? cloud.points.length : 0
      identifier = cloud_identifier(cloud)

      with_clouds_lock do
        if identifier && @cloud_registry.key?(identifier)
          Logger.debug do
            "Облако с идентификатором #{identifier.inspect} уже зарегистрировано"
          end
          return
        end

        ensure_unique_name!(cloud)

        @clouds << cloud
        @cloud_registry[identifier] = cloud if identifier
        cloud.manager = self if cloud.respond_to?(:manager=)
        @active_cloud = cloud
        ensure_overlay!
      end

      Logger.debug do
        "Облако #{cloud.name.inspect} добавлено в менеджер (#{format_point_count(point_count)} точек)"
      end
      log_registered_overlays
      view.invalidate if view
      refresh_ui_panel
    end

    def unregister_cloud!(cloud)
      Threading.guard(:ui, message: 'Manager#unregister_cloud!')
      return unless cloud

      removed_cloud = nil

      with_clouds_lock do
        removed_cloud = @clouds.delete(cloud)
        return unless removed_cloud

        identifier = cloud_identifier(removed_cloud)
        @cloud_registry.delete(identifier) if identifier
        @active_cloud = @clouds.last
      end

      removed_cloud.manager = nil if removed_cloud.respond_to?(:manager=)

      removed_cloud.dispose!
      warn_unless_disposed(removed_cloud)
      view.invalidate if view
      refresh_ui_panel
      Logger.debug do
        "Облако #{removed_cloud&.name.inspect} удалено из менеджера"
      end
    end

    def set_active_cloud!(cloud)
      with_clouds_lock do
        unless cloud.nil? || @clouds.include?(cloud)
          Logger.debug do
            "Попытка активировать незарегистрированное облако: #{cloud.inspect}"
          end
          return @active_cloud
        end

        @active_cloud = cloud
      end

      refresh_ui_panel
      @active_cloud
    end

    def cloud_identifier(cloud)
      return unless cloud

      metadata = cloud.respond_to?(:metadata) ? cloud.metadata : {}
      identifier = metadata[:import_uid]
      identifier || cloud.__id__
    rescue StandardError
      cloud.__id__
    end

    def ensure_unique_name!(cloud)
      return unless cloud.respond_to?(:name)

      base_name = cloud.name.to_s.strip
      base_name = 'Point Cloud' if base_name.empty?

      existing_names = @clouds.map { |existing| existing.name.to_s }
      candidate = base_name
      suffix = 2

      while existing_names.include?(candidate)
        candidate = format('%s (%d)', base_name, suffix)
        suffix += 1
      end

      return if candidate == cloud.name
      return unless cloud.respond_to?(:rename!)

      cloud.rename!(candidate)
    rescue StandardError => e
      Logger.debug { "Не удалось переименовать облако: #{e.class}: #{e.message}" }
    end

    def format_point_count(value)
      count = value.to_i
      if count >= 1_000_000
        formatted = format('%.1fM', count / 1_000_000.0)
        formatted.sub(/\.0M\z/, 'M')
      elsif count >= 1_000
        formatted = format('%.1fK', count / 1_000.0)
        formatted.sub(/\.0K\z/, 'K')
      else
        count.to_s
      end
    rescue StandardError
      value.to_s
    end

    def log_registered_overlays
      model = Sketchup.active_model
      return unless model&.respond_to?(:model_overlays)

      overlays = model.model_overlays.map(&:name)
      Logger.debug do
        "Зарегистрированные overlays: #{overlays.inspect}"
      end
    rescue StandardError => e
      Logger.debug { "Не удалось получить список overlays: #{e.class}: #{e.message}" }
    end

    def warn_unless_disposed(cloud)
      return if cloud.disposed?

      warn("[PointCloudImporter] Облако '#{cloud.name}' не полностью освобождено после dispose!")
    rescue StandardError => e
      warn("[PointCloudImporter] Не удалось проверить состояние облака '#{cloud.name}': #{e.message}")
    end

    def refresh_ui_panel
      return unless defined?(PointCloudImporter::UI::Commands)

      commands = PointCloudImporter::UI::Commands.instance(self)
      return unless commands.respond_to?(:refresh_panel_if_visible)

      commands.refresh_panel_if_visible
    rescue StandardError => e
      warn("[PointCloudImporter] Не удалось обновить панель: #{e.message}")
    end

    def with_clouds_lock
      if @clouds_lock.owned?
        yield
      else
        @clouds_lock.synchronize { yield }
      end
    end

    def draw_preview_measurement(view)
      measurement = @measurement_history.preview_entry
      return unless measurement

      from = measurement[:from]
      to = measurement[:to]
      return unless from && to

      view.line_width = 2
      view.drawing_color = Sketchup::Color.new(0, 170, 255)
      view.draw_polyline(from, to)

      mid_point = Geom.linear_combination(0.5, from, 0.5, to)
      screen = view.screen_coords(mid_point)
      label = measurement[:label] || Sketchup.format_length(measurement[:distance])
      view.draw_text(screen, label, size: 14, color: Sketchup::Color.new(255, 255, 255))
    end
  end
end
