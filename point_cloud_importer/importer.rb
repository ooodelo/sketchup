# encoding: utf-8
# frozen_string_literal: true

require 'stringio'
require 'thread'
require 'securerandom'
require 'uri'

require_relative 'settings'
require_relative 'point_cloud'
require_relative 'ply_parser'
require_relative 'import_job'
require_relative 'logger'
require_relative 'main_thread_queue'
require_relative 'threading'
require_relative 'ui/import_progress_dialog'

module PointCloudImporter
  # High-level importer orchestrating parsing and creation of point clouds.
  class Importer
    Result = Struct.new(:cloud, :duration)

    ChunkPayload = Struct.new(:points,
                              :colors,
                              :intensities,
                              :bounds,
                              :intensity_range,
                              :processed_vertices,
                              :count)

    attr_reader :last_result

    VIEW_INVALIDATE_INTERVAL = 0.2
    RENDER_CACHE_RETRY_INTERVAL = 0.1
    RENDER_CACHE_MAX_ATTEMPTS = 10
    RENDER_CACHE_TIMEOUT = 3.0
    DEFAULT_UNIT_SCALE = 1.0
    MM_TO_INCH = 1.0 / 25.4
    UNIT_SCALE_FACTORS = {
      0 => 1.0, # Inches
      1 => 12.0, # Feet
      2 => MM_TO_INCH, # Millimeters
      3 => MM_TO_INCH * 10.0, # Centimeters
      4 => MM_TO_INCH * 1_000.0, # Meters
      5 => MM_TO_INCH * 1_000_000.0, # Kilometers
      6 => 36.0, # Yards
      7 => 63_360.0 # Miles
    }.freeze

    UNIT_DEFINITIONS = [
      { key: :inch, label: 'Дюймы (in)', scale: 1.0, su_codes: [0] },
      { key: :foot, label: 'Футы (ft)', scale: 12.0, su_codes: [1] },
      { key: :millimeter, label: 'Миллиметры (mm)', scale: MM_TO_INCH, su_codes: [2] },
      { key: :centimeter, label: 'Сантиметры (cm)', scale: MM_TO_INCH * 10.0, su_codes: [3] },
      { key: :meter, label: 'Метры (m)', scale: MM_TO_INCH * 1_000.0, su_codes: [4] },
      { key: :kilometer, label: 'Километры (km)', scale: MM_TO_INCH * 1_000_000.0, su_codes: [5] },
      { key: :yard, label: 'Ярды (yd)', scale: 36.0, su_codes: [6] },
      { key: :mile, label: 'Мили (mi)', scale: 63_360.0, su_codes: [7] }
    ].map(&:freeze).freeze

    UNIT_DEFINITIONS_BY_KEY = UNIT_DEFINITIONS.each_with_object({}) do |definition, memo|
      memo[definition[:key]] = definition
    end.freeze

    UNIT_DEFINITIONS_BY_SU_CODE = UNIT_DEFINITIONS.each_with_object({}) do |definition, memo|
      next unless definition[:su_codes]

      definition[:su_codes].each { |code| memo[code] = definition }
    end.freeze

    UNIT_SYNONYMS = {
      inch: %w[in inch inches дюйм дюймы],
      foot: %w[ft foot feet фут футы],
      millimeter: %w[mm millimeter millimeters миллиметр миллиметры мм],
      centimeter: %w[cm centimeter centimetre centimeters centimetres сантиметр сантиметры см],
      meter: %w[m meter metre meters metres метр метры м],
      kilometer: %w[km kilometer kilometre kilometers kilometres километр километры км],
      yard: %w[yd yard yards ярд ярды],
      mile: %w[mi mile miles миля мили]
    }.freeze

    def initialize(manager)
      @manager = manager
      @last_result = nil
      @last_view_invalidation_at = monotonic_time - VIEW_INVALIDATE_INTERVAL
    end

    def import_from_dialog
      path = ::UI.openpanel('Выберите файл облака точек', nil, 'PLY (*.ply)|*.ply||')
      return unless path

      unit_options = prompt_unit_selection(path)
      return unless unit_options

      options = prompt_options
      return unless options

      import(path, options.merge(unit_options))
    end

    def import(path, options = {})
      options = (options || {}).dup
      settings = Settings.instance
      preset_key = options.delete(:preset) || options[:import_preset] || settings[:import_preset]
      preset_parameters = settings.import_preset_parameters(preset_key)
      options = preset_parameters.merge(options) if preset_parameters
      options[:import_preset] = preset_key if preset_key
      options[:unit_scale] = resolve_unit_scale(options[:unit_scale])
      job = ImportJob.new(path, options)
      run_job(job)
      job
    end

    STYLE_LABELS = {
      square: 'квадрат',
      round: 'круг',
      plus: 'крест'
    }.freeze

    private

    def resolve_unit_scale(preferred)
      extract_valid_unit_scale(preferred) || extract_valid_unit_scale(model_unit_scale) || DEFAULT_UNIT_SCALE
    end

    def sanitized_unit_scale(value)
      extract_valid_unit_scale(value) || DEFAULT_UNIT_SCALE
    end

    def extract_valid_unit_scale(value)
      return nil if value.nil?
      return nil unless value.respond_to?(:to_f)

      scale = value.to_f
      return nil unless scale.finite? && scale.positive?

      scale
    rescue StandardError
      nil
    end

    def unit_definition_for_key(key)
      UNIT_DEFINITIONS_BY_KEY[key]
    end

    def unit_label_for(key)
      unit_definition_for_key(key)&.dig(:label)
    end

    def unit_scale_for(key)
      unit_definition_for_key(key)&.dig(:scale)
    end

    def model_unit_definition
      unit_value = model_length_unit_value
      return nil unless unit_value

      UNIT_DEFINITIONS_BY_SU_CODE[unit_value]
    end

    def model_length_unit_value
      return nil unless defined?(Sketchup) && Sketchup.respond_to?(:active_model)

      model = Sketchup.active_model
      return nil unless model

      units_options = fetch_units_options(model)
      return nil unless units_options

      unit_value = begin
        units_options['LengthUnit']
      rescue StandardError
        nil
      end

      unit_value.respond_to?(:to_i) ? unit_value.to_i : unit_value
    end

    def model_unit_scale
      return DEFAULT_UNIT_SCALE unless defined?(Sketchup) && Sketchup.respond_to?(:active_model)

      model = Sketchup.active_model
      return DEFAULT_UNIT_SCALE unless model

      units_options = fetch_units_options(model)
      return DEFAULT_UNIT_SCALE unless units_options

      unit_value = begin
        units_options['LengthUnit']
      rescue StandardError
        nil
      end

      unit_key = unit_value.respond_to?(:to_i) ? unit_value.to_i : unit_value
      UNIT_SCALE_FACTORS[unit_key] || DEFAULT_UNIT_SCALE
    rescue StandardError
      DEFAULT_UNIT_SCALE
    end

    def fetch_units_options(model)
      return nil unless model.respond_to?(:options)

      options = model.options
      return nil unless options.respond_to?(:[])

      options['UnitsOptions']
    rescue StandardError
      nil
    end

    def prompt_unit_selection(path)
      return {} unless defined?(::UI) && ::UI.respond_to?(:inputbox)

      detected_unit = detect_point_cloud_unit(path)
      model_definition = model_unit_definition
      model_label = model_definition ? model_definition[:label] : unit_label_for(:inch)
      model_option_label = "Текущие настройки модели (#{model_label})"

      source_options_map = { 'Определить автоматически' => :auto }
      UNIT_DEFINITIONS.each { |definition| source_options_map[definition[:label]] = definition[:key] }

      target_options_map = { model_option_label => :model }
      UNIT_DEFINITIONS.each { |definition| target_options_map[definition[:label]] = definition[:key] }

      source_default_label = detected_unit ? unit_label_for(detected_unit) : source_options_map.keys.first
      target_default_label = model_option_label

      prompts = ['Единицы облака точек', 'Единицы модели SketchUp']
      defaults = [source_default_label, target_default_label]
      lists = [source_options_map.keys.join('|'), target_options_map.keys.join('|')]

      selection = ::UI.inputbox(prompts, defaults, lists, 'Настройки единиц и масштаба')
      return unless selection

      source_label, target_label = selection
      source_key = source_options_map[source_label] || :auto
      target_key = target_options_map[target_label] || :model

      selected_source_key = source_key == :auto ? nil : source_key
      selected_target_key = target_key == :model ? nil : target_key

      scale = unit_scale_for(selected_source_key)
      scale ||= unit_scale_for(selected_target_key)

      Logger.debug do
        format(
          'Выбраны единицы: облако=%<source>s, SketchUp=%<target>s, коэффициент=%<scale>.6f',
          source: selected_source_key || :auto,
          target: selected_target_key || :model,
          scale: scale || 0.0
        )
      end

      {
        unit_scale: scale,
        point_unit: selected_source_key,
        sketchup_unit: selected_target_key,
        detected_point_unit: detected_unit
      }
    rescue StandardError => e
      Logger.debug { "Не удалось запросить единицы: #{e.class}: #{e.message}" }
      {}
    end

    def detect_point_cloud_unit(path)
      return nil unless path && File.file?(path)

      header_strings = extract_ply_header_metadata(path)
      return nil if header_strings.empty?

      detect_unit_from_strings(header_strings)
    rescue StandardError => e
      Logger.debug { "Не удалось определить единицы облака: #{e.class}: #{e.message}" }
      nil
    end

    def extract_ply_header_metadata(path)
      strings = []

      File.open(path, 'rb') do |io|
        first_line = io.gets
        return strings unless first_line && first_line.strip.casecmp('ply').zero?

        until (line = io.gets).nil?
          stripped = line.strip
          break if stripped.casecmp('end_header').zero?

          case stripped
          when /^comment\s+(.*)$/i
            strings << sanitize_header_string(Regexp.last_match(1))
          when /^obj_info\s+(.*)$/i
            strings << sanitize_header_string(Regexp.last_match(1))
          end
        end
      end

      strings
    rescue StandardError
      []
    end

    def sanitize_header_string(value)
      return '' if value.nil?

      value = value.to_s
      value = value.dup.force_encoding('UTF-8') if value.encoding != Encoding::UTF_8
      value.encode('UTF-8', invalid: :replace, undef: :replace, replace: '').strip
    rescue StandardError
      value.to_s.strip
    end

    def detect_unit_from_strings(strings)
      strings.each do |raw|
        normalized = raw.to_s.downcase
        next unless normalized.include?('unit') || normalized.include?('scale')

        UNIT_SYNONYMS.each do |key, synonyms|
          synonyms.each do |synonym|
            pattern = /\b#{Regexp.escape(synonym)}s?\b/
            return key if normalized.match?(pattern)
          end
        end

        if (match = normalized.match(/scale\s*[:=]\s*([\d.eE+-]+)/))
          value = match[1].to_f
          detected = UNIT_DEFINITIONS.min_by do |definition|
            (definition[:scale] - value).abs
          end
          if detected && (detected[:scale] - value).abs <= detected[:scale] * 0.05
            return detected[:key]
          end
        end
      end

      nil
    end

    def scale_points!(points, scale)
      return points unless points && !points.empty?
      return points if scale.nil? || (scale - 1.0).abs < Float::EPSILON

      points.each_with_index do |point, index|
        next unless point

        if point.is_a?(Array)
          x = point[0]
          y = point[1]
          z = point[2]
          next if x.nil? || y.nil? || z.nil?

          point[0] = x.to_f * scale
          point[1] = y.to_f * scale
          point[2] = z.to_f * scale
        elsif point.respond_to?(:x) && point.respond_to?(:y) && point.respond_to?(:z)
          points[index] = [point.x.to_f * scale, point.y.to_f * scale, point.z.to_f * scale]
        end
      end

      points
    end

    def extract_point_coordinates(point)
      return nil unless point

      if point.respond_to?(:x) && point.respond_to?(:y) && point.respond_to?(:z)
        [point.x.to_f, point.y.to_f, point.z.to_f]
      elsif point.respond_to?(:[])
        x = point[0]
        y = point[1]
        z = point[2]
        return nil if x.nil? || y.nil? || z.nil?

        [x.to_f, y.to_f, z.to_f]
      end
    rescue StandardError
      nil
    end

    def compute_chunk_bounds(points)
      return nil unless points && !points.empty?

      min_x = Float::INFINITY
      min_y = Float::INFINITY
      min_z = Float::INFINITY
      max_x = -Float::INFINITY
      max_y = -Float::INFINITY
      max_z = -Float::INFINITY

      points.each do |point|
        coords = extract_point_coordinates(point)
        next unless coords

        x, y, z = coords
        min_x = x if x < min_x
        max_x = x if x > max_x
        min_y = y if y < min_y
        max_y = y if y > max_y
        min_z = z if z < min_z
        max_z = z if z > max_z
      end

      return nil if min_x == Float::INFINITY

      {
        min_x: min_x,
        min_y: min_y,
        min_z: min_z,
        max_x: max_x,
        max_y: max_y,
        max_z: max_z
      }
    end

    def merge_bounds(existing, new_bounds)
      return existing unless new_bounds
      unless existing
        return {
          min_x: new_bounds[:min_x],
          min_y: new_bounds[:min_y],
          min_z: new_bounds[:min_z],
          max_x: new_bounds[:max_x],
          max_y: new_bounds[:max_y],
          max_z: new_bounds[:max_z]
        }
      end

      existing[:min_x] = [existing[:min_x], new_bounds[:min_x]].min
      existing[:min_y] = [existing[:min_y], new_bounds[:min_y]].min
      existing[:min_z] = [existing[:min_z], new_bounds[:min_z]].min
      existing[:max_x] = [existing[:max_x], new_bounds[:max_x]].max
      existing[:max_y] = [existing[:max_y], new_bounds[:max_y]].max
      existing[:max_z] = [existing[:max_z], new_bounds[:max_z]].max

      existing
    end

    def compute_intensity_range(values)
      return nil unless values && !values.empty?

      min_value = Float::INFINITY
      max_value = -Float::INFINITY

      values.each do |raw|
        next unless raw.respond_to?(:to_f)

        value = raw.to_f
        next unless value.finite?

        min_value = value if value < min_value
        max_value = value if value > max_value
      end

      return nil if min_value == Float::INFINITY

      { min: min_value, max: max_value }
    end

    def merge_intensity_range(existing, new_range)
      return existing unless new_range
      unless existing
        return { min: new_range[:min], max: new_range[:max] }
      end

      if new_range[:min]
        existing[:min] = [existing[:min], new_range[:min]].min
      end
      if new_range[:max]
        existing[:max] = [existing[:max], new_range[:max]].max
      end

      existing
    end

    def run_job(job)
      Threading.guard(:ui, message: 'Importer#run_job')
      scheduler = MainThreadScheduler.instance
      scheduler.ensure_started
      @last_result = nil
      metrics_enabled_flag = metrics_enabled?
      metrics_state = {
        append_duration: 0.0,
        append_points: 0,
        assign_hooked: false
      }
      metric_logger = method(:log_metric)
      memory_fetcher = method(:capture_peak_memory_bytes)
      cache_point_counter = method(:cache_point_count)
      Logger.debug do
        options_text = job.options.map { |key, value| "#{key}=#{value.inspect}" }.join(', ')
        formatted_options = options_text.empty? ? 'без опций' : options_text
        "Запуск импорта файла #{job.path.inspect} (#{formatted_options})"
      end

      progress_dialog_closed = false
      finalization_complete = false

      progress_dialog = UI::ImportProgressDialog.new(
        job,
        on_cancel: -> { job.cancel! },
        on_close: lambda do
          progress_dialog_closed = true
          if job.finished?
            finalize_job(job, notify: !job.failed?) unless finalization_complete
          else
            job.cancel!
          end
        end,
        on_show_log: lambda do |path = nil|
          candidate = path && !path.to_s.empty? ? path : Logger.log_path
          open_log_file(candidate)
        end
      )
      progress_dialog.show
      MainThreadDispatcher.enqueue { Logger.debug { 'DISPATCH_OK' } }
      scheduler.schedule(name: 'ping', delay: 0.2) { Logger.debug { 'SCHED_OK' } }

      job.on_progress { MainThreadDispatcher.enqueue { progress_dialog.update } }
      job.on_state_change { MainThreadDispatcher.enqueue { progress_dialog.update } }

      cloud_context = {
        cloud: nil,
        invalidate_every_n_chunks: 1,
        chunks_processed: 0,
        start_time: nil,
        chunk_size: nil,
        binary_buffer_size: nil,
        binary_vertex_batch_size: nil,
        last_invalidation_log_time: nil,
        last_invalidation_chunk: 0,
        point_unit: nil,
        sketchup_unit: nil,
        detected_point_unit: nil
      }

      worker_finished = false
      finalize_if_ready = lambda do
        return unless worker_finished && job.finished?
        return if finalization_complete

        if job.failed?
          finalization_complete = true
          finalize_job(job, notify: false)
          return
        end

        return if progress_dialog_closed

        finalization_complete = true
        progress_dialog_closed = true
        progress_dialog.close
        finalize_job(job)
      end

      handle_message = lambda do |message, payload|
        begin
          case message
          when :create_cloud
            return if job.cancel_requested? || job.finished?

            cloud = PointCloud.new(name: payload[:name],
                                   metadata: payload[:metadata] || {},
                                   settings_snapshot: payload[:settings_snapshot])
            job.cloud = cloud
            cloud.begin_import! if cloud.respond_to?(:begin_import!)

            if metrics_enabled_flag && !metrics_state[:assign_hooked]
              metrics_state[:assign_hooked] = install_assign_metrics(
                cloud,
                metric_logger,
                memory_fetcher,
                cache_point_counter,
                metrics_enabled_flag
              )
            end

            cloud_context[:cloud] = cloud
            cloud_context[:invalidate_every_n_chunks] = [payload[:invalidate_every_n_chunks].to_i, 1].max
            cloud_context[:start_time] = payload[:start_time]
            cloud_context[:chunk_size] = payload[:chunk_size]
            cloud_context[:binary_buffer_size] = payload[:binary_buffer_size]
            cloud_context[:binary_vertex_batch_size] = payload[:binary_vertex_batch_size]
            cloud_context[:unit_scale] = sanitized_unit_scale(payload[:unit_scale])
            cloud_context[:point_unit] = payload[:point_unit]
            cloud_context[:sketchup_unit] = payload[:sketchup_unit]
            cloud_context[:detected_point_unit] = payload[:detected_point_unit]
          when :append_chunk
            return if job.cancel_requested? || job.finished?

            cloud = cloud_context[:cloud]
            return unless cloud

            append_started_at = metrics_enabled_flag ? Time.now : nil
            cloud.append_points!(
              payload.points,
              payload.colors,
              payload.intensities,
              bounds: payload.bounds,
              intensity_range: payload.intensity_range
            )
            if metrics_enabled_flag && append_started_at
              metrics_state[:append_duration] += Time.now - append_started_at
              metrics_state[:append_points] += (payload.count || collection_length(payload.points)).to_i
            end

            cloud_context[:chunks_processed] += 1
            invalidate_every = cloud_context[:invalidate_every_n_chunks]
            if invalidate_every.positive? && (cloud_context[:chunks_processed] % invalidate_every).zero?
              invalidated = throttled_view_invalidate
              log_invalidation_metrics(cloud_context) if invalidated
            end
          when :finalize_cloud
            return if job.finished?

            if job.cancel_requested?
              cloud_context[:cloud]&.abort_import!
              job.mark_cancelled!
              finalize_if_ready.call
              return
            end

            cloud = cloud_context[:cloud]
            return unless cloud

            cloud.finalize_bounds!
            cloud.complete_import! if cloud.respond_to?(:complete_import!)

            metadata = (payload[:metadata] || {}).dup
            unit_scale = cloud_context[:unit_scale]
            importer_metadata = metadata['point_cloud_importer']
            importer_metadata = importer_metadata.is_a?(Hash) ? importer_metadata.dup : {}
            importer_metadata['unit_scale'] = unit_scale if unit_scale && (unit_scale - 1.0).abs >= Float::EPSILON
            importer_metadata['point_unit'] = cloud_context[:point_unit].to_s if cloud_context[:point_unit]
            importer_metadata['sketchup_unit'] = cloud_context[:sketchup_unit].to_s if cloud_context[:sketchup_unit]
            importer_metadata['detected_point_unit'] = cloud_context[:detected_point_unit].to_s if cloud_context[:detected_point_unit]
            metadata['point_cloud_importer'] = importer_metadata unless importer_metadata.empty?
            cloud.update_metadata!(metadata)

            completion_time = Time.now
            parsing_started_at = payload[:parsing_started_at]
            parsing_finished_at = parsing_started_at ? (payload[:parsing_finished_at] || completion_time) : nil
            parsing_total_duration = if parsing_started_at && parsing_finished_at
                                       parsing_finished_at - parsing_started_at
                                     end
            append_duration = metrics_state[:append_duration]
            parsing_duration = if parsing_total_duration && append_duration
                                 parsing_total_duration - append_duration
                               else
                                 parsing_total_duration
                               end
            parsing_duration = 0.0 if parsing_duration && parsing_duration.negative?
            total_points = collection_length(cloud.points)
            peak_memory_bytes = memory_fetcher.call

            if metrics_enabled_flag && parsing_started_at
              metric_logger.call(
                'parsing',
                parsing_duration,
                points: total_points,
                peak_memory_bytes: peak_memory_bytes,
                enabled: metrics_enabled_flag
              )
              metric_logger.call(
                'append_points!',
                append_duration,
                points: metrics_state[:append_points],
                peak_memory_bytes: peak_memory_bytes,
                enabled: metrics_enabled_flag
              )
            end

            total_vertices = payload[:total_vertices].to_i
            total_vertices = cloud.points.length if total_vertices.zero?
            start_time = cloud_context[:start_time]
            total_duration = start_time ? completion_time - start_time : nil

            job.transition_to(:register)
            job.complete!(
              cloud: cloud,
              metadata: metadata,
              duration: total_duration,
              total_vertices: total_vertices
            )

            log_import_summary(
              parse_duration: parsing_duration,
              append_duration: append_duration,
              total_duration: total_duration,
              total_points: total_points,
              peak_memory_bytes: peak_memory_bytes,
              chunk_size: cloud_context[:chunk_size],
              binary_buffer_size: cloud_context[:binary_buffer_size],
              binary_vertex_batch_size: cloud_context[:binary_vertex_batch_size],
              invalidate_every: cloud_context[:invalidate_every_n_chunks]
            )

            finalize_if_ready.call
          when :failed
            error = payload
            unless job.finished?
              error ||= RuntimeError.new('Фоновый шаг завершился с ошибкой')
              cloud_context[:cloud]&.abort_import!
              job.fail!(error)
            end
            finalize_if_ready.call
          when :worker_finished
            worker_finished = true
            finalize_if_ready.call
          end
        rescue StandardError => e
          unless job.finished?
            job.fail!(e)
          end
          finalize_if_ready.call
        end
      end

      dispatch = lambda do |message, payload = nil|
        MainThreadDispatcher.enqueue { handle_message.call(message, payload) }
      end

      worker = Threading.run_background(
        name: 'import_worker',
        dispatcher: dispatch,
        on_failure: ->(error) { job.fail!(error) unless job.finished? }
      ) do
        begin
          Threading.guard(:bg, message: 'import_worker')
          job.start!
          job.transition_async(:parsing)
          job.update_progress(processed_vertices: 0, message: 'Чтение PLY...')
          start_time = Time.now
          parser = PlyParser.new(
            job.path,
            progress_callback: job.progress_callback,
            cancelled_callback: -> { job.cancel_requested? }
          )

          point_unit = job.options[:point_unit] || job.options[:detected_point_unit]
          sketchup_unit = job.options[:sketchup_unit]
          unit_scale = sanitized_unit_scale(job.options[:unit_scale])
          Logger.debug do
            format(
              'Импорт: коэффициент масштабирования координат %.6f (облако=%s, SketchUp=%s)',
              unit_scale,
              point_unit || :auto,
              sketchup_unit || :model
            )
          end

          job.update_progress(
            processed_vertices: 0,
            total_vertices: parser.total_vertex_count,
            total_bytes: parser.progress_estimator.total_bytes,
            message: 'Чтение PLY...'
          )

          name = File.basename(job.path, '.*')
          settings = Settings.instance
          settings_snapshot = settings.snapshot
          config = defined?(PointCloudImporter::Config) ? PointCloudImporter::Config : nil

          default_chunk_size = config&.chunk_size || 1_000_000
          chunk_size_source = job.options[:chunk_size] || settings[:import_chunk_size] || default_chunk_size
          chunk_size = if config
                         config.sanitize_chunk_size(chunk_size_source)
                       else
                         value = chunk_size_source || default_chunk_size
                         value = value.to_i
                         value = 1 if value < 1
                         value
                       end

          default_invalidation = config&.invalidate_every_n_chunks || 1
          invalidate_source = job.options[:invalidate_every_n_chunks] ||
                              settings[:invalidate_every_n_chunks] ||
                              default_invalidation
          invalidate_every_n_chunks = if config
                                         config.sanitize_invalidate_every_n_chunks(invalidate_source)
                                       else
                                         value = invalidate_source || default_invalidation
                                         value = value.to_i
                                         value = 1 if value < 1
                                         value
                                       end

          default_buffer_size = config&.binary_buffer_size || settings[:binary_buffer_size] ||
                                PlyParser::BINARY_READ_BUFFER_SIZE
          buffer_source = job.options[:binary_buffer_size]
          buffer_source = settings[:binary_buffer_size] if buffer_source.nil?
          buffer_size = if config
                          config.sanitize_binary_buffer_size(buffer_source || default_buffer_size)
                        else
                          sanitize_positive_integer(buffer_source, default_buffer_size)
                        end

          default_batch_size = config&.binary_vertex_batch_size || settings[:binary_vertex_batch_size] ||
                               PlyParser::DEFAULT_BINARY_VERTEX_BATCH_SIZE
          batch_source = job.options[:binary_vertex_batch_size]
          batch_source = settings[:binary_vertex_batch_size] if batch_source.nil?
          batch_size_preference = if config
                                     config.sanitize_binary_vertex_batch_size(batch_source || default_batch_size)
                                   else
                                     sanitize_positive_integer(batch_source, default_batch_size)
                                   end
          batch_size_preference = clamp(batch_size_preference,
                                        PlyParser::BINARY_VERTEX_BATCH_MIN,
                                        PlyParser::BINARY_VERTEX_BATCH_MAX)

          Logger.debug do
            format(
              'Импорт: chunk_size=%<chunk>d, binary_buffer=%<buffer>s, vertex_batch=%<batch>d, invalidate_every=%<invalidate>d',
              chunk: chunk_size,
              buffer: format_memory(buffer_size),
              batch: batch_size_preference,
              invalidate: invalidate_every_n_chunks
            )
          end

          dispatch.call(
            :create_cloud,
            {
              name: name,
              metadata: parser.metadata || {},
              invalidate_every_n_chunks: invalidate_every_n_chunks,
              start_time: start_time,
              chunk_size: chunk_size,
              binary_buffer_size: buffer_size,
              binary_vertex_batch_size: batch_size_preference,
              unit_scale: unit_scale,
              point_unit: point_unit,
              sketchup_unit: sketchup_unit,
              detected_point_unit: job.options[:detected_point_unit],
              settings_snapshot: settings_snapshot
            }
          )

          parsing_started_at = metrics_enabled_flag ? Time.now : nil
          metadata = parser.parse(chunk_size: chunk_size) do |points_chunk, colors_chunk, intensities_chunk, processed|
            break if job.cancel_requested? || job.finished?
            job.ensure_state_fresh!
            next unless points_chunk && !points_chunk.empty?

            scale_points!(points_chunk, unit_scale) if unit_scale != 1.0

            chunk_bounds = compute_chunk_bounds(points_chunk)
            chunk_range = if intensities_chunk && !intensities_chunk.empty?
                            compute_intensity_range(intensities_chunk)
                          end

            payload = ChunkPayload.new(
              points_chunk,
              colors_chunk && !colors_chunk.empty? ? colors_chunk : nil,
              intensities_chunk && !intensities_chunk.empty? ? intensities_chunk : nil,
              chunk_bounds,
              chunk_range,
              processed,
              points_chunk.length
            )

            dispatch.call(:append_chunk, payload)

            total_vertices = parser.total_vertex_count.to_i
            total_vertices = processed if total_vertices.zero? || total_vertices < processed
            job.update_progress(
              processed_vertices: processed,
              total_vertices: total_vertices,
              message: format_progress_message(processed, total_vertices)
            )
          end

          parsing_finished_at = metrics_enabled_flag ? Time.now : nil

          if job.cancel_requested?
            job.mark_cancelled!
          elsif job.finished?
            # already handled
          else
            job.transition_async(:sampling)
            job.transition_async(:build_index)
            dispatch.call(
              :finalize_cloud,
              {
                metadata: metadata || parser.metadata,
                total_vertices: parser.total_vertex_count.to_i,
                parsing_started_at: parsing_started_at,
                parsing_finished_at: parsing_finished_at
              }
            )
          end
        rescue PlyParser::Cancelled
          job.mark_cancelled!
        rescue PlyParser::UnsupportedFormat => e
          job.fail!(e)
          raise
        rescue ImportJob::TimeoutError => e
          job.fail!(e)
          raise
        ensure
          dispatch.call(:worker_finished)
        end
      end

      job.thread = worker
    end

    def finalize_job(job, notify: true)
      Logger.debug { "Финализация задания со статусом #{job.status.inspect}" }
      case job.status
      when :completed
        finalize_completed_job(job)
      when :failed
        cleanup_partial_cloud(job)
        log_failure_details(job)
        @last_result = nil
        ::UI.messagebox("Ошибка импорта: #{job.error.message}") if notify && job.error
      when :cancelled
        cleanup_partial_cloud(job)
        @last_result = nil
        ::UI.messagebox('Импорт отменен пользователем.') if notify
      else
        @last_result = nil
      end
    end

    def throttled_view_invalidate
      view = @manager&.view
      return false unless view && view.respond_to?(:invalidate)

      now = monotonic_time
      elapsed = now - @last_view_invalidation_at

      if elapsed < VIEW_INVALIDATE_INTERVAL
        Logger.debug do
          format(
            'Пропуск инвалидации вида: прошло %.3f с (порог %.3f с)',
            elapsed,
            VIEW_INVALIDATE_INTERVAL
          )
        end
        return false
      end

      view.invalidate
      @last_view_invalidation_at = now
      true
    rescue StandardError => e
      Logger.debug { "Не удалось выполнить инвалидацию вида: #{e.class}: #{e.message}" }
      false
    end

    def log_invalidation_metrics(context)
      return unless context

      now = monotonic_time
      last_time = context[:last_invalidation_log_time] || context[:start_time] || now
      elapsed = now - last_time
      chunks_since_last = context[:chunks_processed] - context[:last_invalidation_chunk].to_i
      invalidate_every = context[:invalidate_every_n_chunks]

      invalidation_rate = elapsed.positive? ? (1.0 / elapsed) : 0.0
      chunk_rate = elapsed.positive? ? (chunks_since_last.to_f / elapsed) : 0.0

      Logger.debug do
        format(
          'Инвалидация вида: chunk=%<chunk>d, interval=%.3fs, invalidations_per_s=%.2f, chunk_rate=%.2f/с (каждые %<every>d чанков)',
          chunk: context[:chunks_processed],
          interval: elapsed,
          invalidations_per_s: invalidation_rate,
          chunk_rate: chunk_rate,
          every: invalidate_every
        )
      end

      context[:last_invalidation_log_time] = now
      context[:last_invalidation_chunk] = context[:chunks_processed]
    end

    def log_import_summary(parse_duration:, append_duration:, total_duration:, total_points:, peak_memory_bytes:, chunk_size:,
                           binary_buffer_size:, binary_vertex_batch_size:, invalidate_every:)
      Logger.debug do
        parts = ['Импорт завершен']
        parts << "total=#{format_duration(total_duration)}" if total_duration
        parts << "parse=#{format_duration(parse_duration)}" if parse_duration
        parts << "append=#{format_duration(append_duration)}" if append_duration
        parts << "points=#{format_point_count(total_points)}" if total_points && total_points.positive?
        parts << "chunk_size=#{format_point_count(chunk_size)}" if chunk_size && chunk_size.positive?
        parts << "buffer=#{format_memory(binary_buffer_size)}" if binary_buffer_size
        parts << "vertex_batch=#{binary_vertex_batch_size}" if binary_vertex_batch_size
        parts << "invalidate_every=#{invalidate_every}" if invalidate_every
        parts << "peak_memory=#{format_memory(peak_memory_bytes)}" if peak_memory_bytes
        parts.join(', ')
      end
    end

    def finalize_completed_job(job)
      data = job.result || {}
      cloud = data[:cloud]
      metadata = (data[:metadata] || {}).dup
      duration = data[:duration]
      total_vertices = data[:total_vertices]

      raise ArgumentError, 'PLY файл не содержит точек' unless cloud && cloud.points && cloud.points.length.positive?

      metadata[:import_uid] ||= SecureRandom.uuid

      background_metrics_enabled = metrics_enabled?
      background_metric_logger = method(:log_metric)
      background_memory_fetcher = method(:capture_peak_memory_bytes)

      result_struct = Result.new(cloud, duration)

      steps = []

      steps << lambda do
        cloud.update_metadata!(metadata)
        apply_visual_options(cloud, job.options)
      end

      steps << lambda do
        next if job.cloud_added?

        @manager.add_cloud(cloud)
        job.mark_cloud_added!
      end

      steps << lambda do
        next unless cloud.respond_to?(:mark_render_cache_preparation_pending!)

        pending = cloud.mark_render_cache_preparation_pending!
        next unless pending

        schedule_render_cache_preparation(
          cloud,
          metrics_enabled: background_metrics_enabled,
          metric_logger: background_metric_logger,
          memory_fetcher: background_memory_fetcher
        )
      end

      steps << lambda do
        unless cloud.respond_to?(:mark_render_cache_preparation_pending!)
          schedule_render_cache_preparation(
            cloud,
            metrics_enabled: background_metrics_enabled,
            metric_logger: background_metric_logger,
            memory_fetcher: background_memory_fetcher
          )
        end

        duration_value = duration ? format('%.2f', duration) : '0.00'
        imported = format_point_count(cloud.points.length)
        total = format_point_count(total_vertices)
        message = "Импорт завершен за #{duration_value} сек. " \
                  "Импортировано #{imported} из #{total} точек"
        ::UI.messagebox(message)
        @last_result = result_struct
      end

      enqueue_sequence(steps)
      nil
    rescue StandardError => e
      @last_result = nil
      ::UI.messagebox("Ошибка импорта: #{e.message}")
      nil
    end

    def enqueue_sequence(steps)
      steps = steps.compact
      return if steps.empty?

      step = steps.shift
      MainThreadDispatcher.enqueue do
        begin
          step.call
        rescue StandardError => error
          Logger.debug do
            "Ошибка при выполнении шага финализации: #{error.class}: #{error.message}"
          end
        ensure
          enqueue_sequence(steps) unless steps.empty?
        end
      end
    end
    private :enqueue_sequence

    def log_failure_details(job)
      Logger.error do
        error = job.error
        next 'Сообщение об ошибке отсутствует' unless error

        backtrace = Array(error.backtrace).first(5).join(' | ')
        "Задание завершилось ошибкой: #{error.class}: #{error.message} (#{backtrace})"
      end
    end

    def apply_visual_options(cloud, options)
      Logger.debug("Применение визуальных опций: #{options.inspect}")
      cloud.density = options[:display_density] if options[:display_density]
      cloud.point_size = options[:point_size] if options[:point_size]
      cloud.point_style = options[:point_style] if options[:point_style]
      cloud.max_display_points = options[:max_display_points] if options[:max_display_points]
    end

    def schedule_render_cache_preparation(cloud, metrics_enabled:, metric_logger:, memory_fetcher:)
      Threading.guard(:ui, message: 'Importer#schedule_render_cache_preparation')
      return unless cloud

      attempts = 0
      metrics_start = metrics_enabled ? Time.now : nil
      start_monotonic = monotonic_time
      task_handle = nil

      finalize = lambda do |success|
        cloud.clear_render_cache_preparation_pending! if cloud.respond_to?(:clear_render_cache_preparation_pending!)

        if success && metrics_enabled && metrics_start
          index_duration = Time.now - metrics_start
          points_count = collection_length(cloud&.points)
          metric_logger.call(
            'background_index',
            index_duration,
            points: points_count,
            peak_memory_bytes: memory_fetcher.call,
            enabled: metrics_enabled
          )
        end

        if task_handle
          task_handle.cancel
          task_handle = nil
        end
      end

      attempt = nil
      attempt = lambda do
        attempts += 1
        success = false
        begin
          cloud.prepare_render_cache!
          throttled_view_invalidate
          success = true
        rescue StandardError => error
          Logger.debug do
            "Не удалось подготовить рендер-кэш (попытка #{attempts}): #{error.message}"
          end
        end

        elapsed = monotonic_time - start_monotonic

        if success
          finalize.call(true)
          true
        elsif attempts >= RENDER_CACHE_MAX_ATTEMPTS || elapsed >= RENDER_CACHE_TIMEOUT
          Logger.debug do
            format(
              'Подготовка рендер-кэша прервана после %<attempts>d попыток (%.2f с)',
              attempts: attempts,
              elapsed: elapsed
            )
          end
          finalize.call(false)
          true
        else
          false
        end
      end

      completed = attempt.call
      return if completed

      scheduler = MainThreadScheduler.instance
      task_handle = scheduler.schedule(name: 'render-cache-preparation',
                                       priority: 150,
                                       delay: RENDER_CACHE_RETRY_INTERVAL) do |context|
        continue = attempt.call
        if continue
          :done
        else
          context.reschedule_in = RENDER_CACHE_RETRY_INTERVAL
          :pending
        end
      end
    end

    def open_log_file(path)
      return false unless path && !path.to_s.empty?
      return false unless defined?(::UI) && ::UI.respond_to?(:openURL)

      normalized = path.to_s.encode('UTF-8')
      normalized = normalized.tr('\\', '/')
      escaped = URI::DEFAULT_PARSER.escape(normalized, /[^-\w.\/:]/)
      url = if escaped.start_with?('file://')
              escaped
            elsif escaped.start_with?('/')
              "file://#{escaped}"
            else
              "file:///#{escaped}"
            end

      ::UI.openURL(url)
      true
    rescue StandardError => e
      Logger.debug { "Не удалось открыть лог импорта: #{e.class}: #{e.message}" }
      false
    end

    def prompt_options
      settings = Settings.instance
      prompts = [
        'Размер точки (1-10)',
        'Стиль точки',
        'Доля отображаемых точек (0.01-1.0)',
        'Максимум точек в кадре (10000-8000000)'
      ]
      defaults = [
        settings[:point_size],
        display_name(settings[:point_style]),
        settings[:density],
        settings[:max_display_points]
      ]
      style_list = available_style_labels.join('|')
      lists = ['', style_list, '', '']
      input = ::UI.inputbox(prompts, defaults, lists, 'Настройки визуализации')
      return unless input

      point_size, style_label, density, max_points = input
      {
        point_size: point_size.to_i,
        point_style: resolve_style(style_label),
        display_density: density.to_f,
        max_display_points: max_points.to_i
      }
    end

    def display_name(style)
      style = style.to_sym rescue nil
      style = PointCloud.default_point_style unless PointCloud.available_point_style?(style)
      STYLE_LABELS.fetch(style, STYLE_LABELS.fetch(PointCloud.default_point_style, 'квадрат'))
    rescue StandardError
      STYLE_LABELS.fetch(PointCloud.default_point_style, 'квадрат')
    end

    def resolve_style(value)
      return value if value.is_a?(Symbol) && PointCloud.available_point_style?(value)

      normalized = value.to_s.downcase
      STYLE_LABELS.each do |symbol, label|
        next unless PointCloud.available_point_style?(symbol)

        return symbol if label == normalized
      end

      style = case normalized
              when 'square' then :square
              when 'round' then :round
              when 'plus' then :plus
              when 'крест' then :plus
              when 'круг' then :round
              else
                PointCloud.default_point_style
              end

      PointCloud.available_point_style?(style) ? style : PointCloud.default_point_style
    end

    def available_style_labels
      labels = PointCloud.available_point_styles.filter_map { |style| STYLE_LABELS[style] }
      return labels unless labels.empty?

      [STYLE_LABELS.fetch(PointCloud.default_point_style, 'квадрат')]
    end

    def format_progress_message(processed, total)
      total = [total.to_i, processed.to_i].max
      return "Загружено #{format_point_count(processed)} точек" if total <= 0

      percent = if total.positive?
                  ((processed.to_f / total) * 100).round
                else
                  0
                end
      "Загружено #{format_point_count(processed)} из #{format_point_count(total)} точек (#{percent}%)"
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
    end

    def log_metric(stage, duration, points: nil, peak_memory_bytes: nil, enabled: nil)
      enabled = metrics_enabled? if enabled.nil?
      return unless enabled

      message_parts = ["METRICS #{stage}"]
      message_parts << "duration=#{duration ? format_duration(duration) : 'n/a'}"

      unless points.nil?
        points_value = points.to_i
        message_parts << "points=#{format_point_count(points_value)}"
        if duration && duration.positive?
          rate = points_value.zero? ? 0.0 : points_value.to_f / duration
          message_parts << "rate=#{format_rate(rate)} pts/s"
        end
      end

      if peak_memory_bytes
        message_parts << "peak_memory=#{format_memory(peak_memory_bytes)}"
      end

      Logger.debug(message_parts.join(', '))
    end

    def metrics_enabled?
      return false unless defined?(PointCloudImporter::Config)

      config = PointCloudImporter::Config
      config.respond_to?(:metrics_enabled?) && config.metrics_enabled?
    rescue StandardError
      false
    end

    def sanitize_positive_integer(value, default)
      candidate =
        case value
        when nil
          nil
        when Integer
          value
        when Numeric
          value.to_i
        else
          Integer(value)
        end
      candidate = nil if candidate.nil? || candidate < 1
      candidate || [default, 1].max
    rescue ArgumentError, TypeError
      [default, 1].max
    end

    def clamp(value, min_value, max_value)
      return min_value if value.nil?

      [[value, min_value].max, max_value].min
    end

    def format_duration(value)
      strip_trailing_zero(format('%.3f', value.to_f)) + 's'
    end

    def format_rate(value)
      return '0.00' unless value && value.finite?

      rate = value.to_f
      if rate >= 1_000_000
        strip_trailing_zero(format('%.2fM', rate / 1_000_000.0))
      elsif rate >= 1_000
        strip_trailing_zero(format('%.2fK', rate / 1_000.0))
      else
        strip_trailing_zero(format('%.2f', rate))
      end
    end

    def format_memory(bytes)
      return 'n/a' unless bytes

      value = bytes.to_f
      if value >= 1024**3
        strip_trailing_zero(format('%.2f', value / (1024.0**3))) + ' GB'
      elsif value >= 1024**2
        strip_trailing_zero(format('%.2f', value / (1024.0**2))) + ' MB'
      elsif value >= 1024
        strip_trailing_zero(format('%.2f', value / 1024.0)) + ' KB'
      else
        strip_trailing_zero(format('%.2f', value)) + ' B'
      end
    end

    def strip_trailing_zero(value)
      string = value.to_s
      string = string.sub(/(\.\d*[1-9])0+\z/, '\1')
      string.sub(/\.0+\z/, '')
    end

    def capture_peak_memory_bytes
      status_path = '/proc/self/status'
      return nil unless File.readable?(status_path)

      line = File.foreach(status_path).find { |content| content.start_with?('VmHWM:') }
      return nil unless line

      match = line.match(/VmHWM:\s*(\d+)\s*kB/i)
      return nil unless match

      match[1].to_i * 1024
    rescue StandardError
      nil
    end

    def collection_length(collection)
      return nil unless collection

      collection.respond_to?(:length) ? collection.length : nil
    rescue StandardError
      nil
    end

    def cache_point_count(cache)
      return nil unless cache.is_a?(Hash)

      indices = cache[:point_indices]
      return collection_length(indices) if indices

      collection_length(cache[:points])
    rescue StandardError
      nil
    end

    def monotonic_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    rescue NameError, Errno::EINVAL
      Process.clock_gettime(:float_second)
    rescue NameError, ArgumentError, Errno::EINVAL
      Process.clock_gettime(Process::CLOCK_REALTIME)
    rescue NameError, Errno::EINVAL
      Time.now.to_f
    end

    def install_assign_metrics(cloud, metric_logger, memory_fetcher, cache_point_counter, enabled)
      return false unless enabled

      original_assign = cloud.method(:assign_primary_cache)
      logged = false

      cloud.define_singleton_method(:assign_primary_cache) do |cache|
        start_time = Time.now
        result = original_assign.call(cache)
        unless logged
          logged = true
          metric_logger.call(
            'assign_primary_cache',
            Time.now - start_time,
            points: cache_point_counter.call(cache),
            peak_memory_bytes: memory_fetcher.call,
            enabled: enabled
          )
        end
        result
      end

      true
    rescue NameError
      false
    end

    def cleanup_partial_cloud(job)
      Logger.debug('Начата очистка частично импортированного облака')
      cloud = job.cloud
      return unless cloud

      if job.cloud_added?
        @manager.remove_cloud(cloud)
      else
        cloud.dispose!
      end
    rescue StandardError
      # Best effort cleanup; ignore errors to avoid masking original failure.
    ensure
      job.cloud = nil
      Logger.debug('Очистка частичного облака завершена')
    end

  end
end
