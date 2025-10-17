# encoding: utf-8
# frozen_string_literal: true

require 'stringio'
require 'thread'

require_relative 'settings'
require_relative 'point_cloud'
require_relative 'ply_parser'
require_relative 'import_job'
require_relative 'logger'
require_relative 'ui/import_progress_dialog'

module PointCloudImporter
  # High-level importer orchestrating parsing and creation of point clouds.
  class Importer
    Result = Struct.new(:cloud, :duration)

    attr_reader :last_result

    VIEW_INVALIDATE_INTERVAL = 0.2
    ACCUMULATED_CHUNK_MAX_INTERVAL = 0.1
    RENDER_CACHE_RETRY_INTERVAL = 0.1
    RENDER_CACHE_MAX_ATTEMPTS = 10
    RENDER_CACHE_TIMEOUT = 3.0

    def initialize(manager)
      @manager = manager
      @last_result = nil
      @last_view_invalidation_at = monotonic_time - VIEW_INVALIDATE_INTERVAL
    end

    def import_from_dialog
      path = ::UI.openpanel('Выберите файл облака точек', nil, 'PLY (*.ply)|*.ply||')
      return unless path

      options = prompt_options
      return unless options

      import(path, options)
    end

    def import(path, options = {})
      options = (options || {})
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

    def run_job(job)
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

      progress_dialog = UI::ImportProgressDialog.new(job) { job.cancel! }
      progress_dialog.show

      message_queue = SizedQueue.new(16)
      worker_finished = false
      worker = Thread.new do
        Thread.current.abort_on_exception = false
        begin
          job.start!
          job.update_progress(processed_vertices: 0, message: 'Чтение PLY...')
          start_time = Time.now
          parser = PlyParser.new(
            job.path,
            progress_callback: job.progress_callback,
            cancelled_callback: -> { job.cancel_requested? }
          )

          job.update_progress(
            processed_vertices: 0,
            total_vertices: parser.total_vertex_count,
            total_bytes: parser.progress_estimator.total_bytes,
            message: 'Чтение PLY...'
          )

          name = File.basename(job.path, '.*')
          settings = Settings.instance
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

          Logger.debug do
            format(
              'Импорт: chunk_size=%<chunk>d, binary_buffer=%<buffer>s, vertex_batch=%<batch>d, invalidate_every=%<invalidate>d',
              chunk: chunk_size,
              buffer: format_memory(buffer_size),
              batch: batch_size_preference,
              invalidate: invalidate_every_n_chunks
            )
          end

          message_queue << [
            :create_cloud,
            {
              name: name,
              metadata: parser.metadata || {},
              invalidate_every_n_chunks: invalidate_every_n_chunks,
              start_time: start_time,
              chunk_size: chunk_size,
              binary_buffer_size: buffer_size,
              binary_vertex_batch_size: batch_size_preference
            }
          ]

          chunks_processed = 0
          accumulated_points = nil
          accumulated_colors = nil
          accumulated_intensities = nil
          accumulated_count = 0
          accumulation_started_at = nil

          flush_accumulated = lambda do
            return unless accumulated_points && !accumulated_points.empty?

            colors_payload = accumulated_colors
            colors_payload = nil if colors_payload.is_a?(Array) && colors_payload.empty?
            intensities_payload = accumulated_intensities
            intensities_payload = nil if intensities_payload.is_a?(Array) && intensities_payload.empty?

            message_queue << [
              :append_chunk,
              {
                points: accumulated_points,
                colors: colors_payload,
                intensities: intensities_payload
              }
            ]

            accumulated_points = nil
            accumulated_colors = nil
            accumulated_intensities = nil
            accumulated_count = 0
            accumulation_started_at = nil
            chunks_processed += 1
          end
          parsing_started_at = metrics_enabled_flag ? Time.now : nil
          metadata = parser.parse(chunk_size: chunk_size) do |points_chunk, colors_chunk, intensities_chunk, processed|
            break if job.cancel_requested? || job.finished?
            next unless points_chunk && !points_chunk.empty?

          if accumulated_points
            accumulated_points.concat(points_chunk)
          else
            accumulated_points = points_chunk
            accumulation_started_at = monotonic_time
          end

          if colors_chunk && !colors_chunk.empty?
            if accumulated_colors
              accumulated_colors.concat(colors_chunk)
            else
              accumulated_colors = colors_chunk
            end
          end

          if intensities_chunk && !intensities_chunk.empty?
            if accumulated_intensities
              accumulated_intensities.concat(intensities_chunk)
            else
              accumulated_intensities = intensities_chunk
            end
          end

          accumulated_count += points_chunk.length

          now = monotonic_time
          if accumulated_count >= chunk_size || (accumulation_started_at && now - accumulation_started_at >= ACCUMULATED_CHUNK_MAX_INTERVAL)
            flush_accumulated.call
          end

          total_vertices = parser.total_vertex_count.to_i
          total_vertices = processed if total_vertices.zero? || total_vertices < processed
          job.update_progress(
            processed_vertices: processed,
            total_vertices: total_vertices,
            message: format_progress_message(processed, total_vertices)
          )
          end

          flush_accumulated.call

          parsing_finished_at = metrics_enabled_flag ? Time.now : nil

          if job.cancel_requested?
            job.mark_cancelled!
          elsif job.finished?
            # Job already finished by the main thread (likely due to an error)
          else
            message_queue << [
              :finalize_cloud,
              {
                metadata: metadata || parser.metadata,
                total_vertices: parser.total_vertex_count.to_i,
                parsing_started_at: parsing_started_at,
                parsing_finished_at: parsing_finished_at
              }
            ]
          end
        rescue PlyParser::Cancelled
          job.mark_cancelled!
        rescue PlyParser::UnsupportedFormat => e
          job.fail!(e)
        rescue StandardError => e
          job.fail!(e)
        ensure
          message_queue << [:worker_finished, nil]
        end
      end

      job.thread = worker

      cloud_context = {
        cloud: nil,
        invalidate_every_n_chunks: 1,
        chunks_processed: 0,
        start_time: nil,
        chunk_size: nil,
        binary_buffer_size: nil,
        binary_vertex_batch_size: nil,
        last_invalidation_log_time: nil,
        last_invalidation_chunk: 0
      }

      process_messages = lambda do |limit = nil|
        processed = 0
        loop do
          break if limit && processed >= limit

          message, payload = message_queue.pop(true)
          processed += 1
          begin
            case message
          when :create_cloud
            next if job.cancel_requested? || job.finished?

            cloud = PointCloud.new(name: payload[:name], metadata: payload[:metadata] || {})
            job.cloud = cloud

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
          when :append_chunk
            next if job.cancel_requested? || job.finished?

            cloud = cloud_context[:cloud]
            next unless cloud

            append_started_at = metrics_enabled_flag ? Time.now : nil
            cloud.append_points!(payload[:points], payload[:colors], payload[:intensities])
            if metrics_enabled_flag && append_started_at
              metrics_state[:append_duration] += Time.now - append_started_at
              metrics_state[:append_points] += collection_length(payload[:points]).to_i
            end

            cloud_context[:chunks_processed] += 1
            invalidate_every = cloud_context[:invalidate_every_n_chunks]
            if invalidate_every.positive? && (cloud_context[:chunks_processed] % invalidate_every).zero?
              invalidated = throttled_view_invalidate
              log_invalidation_metrics(cloud_context) if invalidated
            end
          when :finalize_cloud
            next if job.finished?

            if job.cancel_requested?
              job.mark_cancelled!
              next
            end

            cloud = cloud_context[:cloud]
            next unless cloud

            cloud.finalize_bounds!

            metadata = payload[:metadata] || {}
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
          when :worker_finished
            worker_finished = true
          else
            # Ignore unknown messages
          end
          rescue StandardError => e
            unless job.finished?
              job.fail!(e)
            end
          end
        rescue ThreadError
          break
        end
      end

      timer_id = nil
      timer_id = ::UI.start_timer(0.1, repeat: true) do
        progress_dialog.update
        process_messages.call(10)

        next unless worker_finished && job.finished?

        ::UI.stop_timer(timer_id) if timer_id
        progress_dialog.close
        process_messages.call while !message_queue.empty?
        job.thread&.join
        finalize_job(job)
      end
    end

    def finalize_job(job)
      Logger.debug { "Финализация задания со статусом #{job.status.inspect}" }
      case job.status
      when :completed
        finalize_completed_job(job)
      when :failed
        cleanup_partial_cloud(job)
        log_failure_details(job)
        @last_result = nil
        ::UI.messagebox("Ошибка импорта: #{job.error.message}") if job.error
      when :cancelled
        cleanup_partial_cloud(job)
        @last_result = nil
        ::UI.messagebox('Импорт отменен пользователем.')
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
      data = job.result
      cloud = data[:cloud]
      metadata = data[:metadata]
      duration = data[:duration]
      total_vertices = data[:total_vertices]

      raise ArgumentError, 'PLY файл не содержит точек' unless cloud && cloud.points && cloud.points.length.positive?

      cloud.update_metadata!(metadata)
      apply_visual_options(cloud, job.options)

      unless job.cloud_added?
        @manager.add_cloud(cloud)
        job.mark_cloud_added!
      end

      background_metrics_enabled = metrics_enabled?
      background_metric_logger = method(:log_metric)
      background_memory_fetcher = method(:capture_peak_memory_bytes)

      schedule_cache_preparation = if cloud.respond_to?(:mark_render_cache_preparation_pending!)
                                     cloud.mark_render_cache_preparation_pending!
                                   else
                                     true
                                   end

      if schedule_cache_preparation
        schedule_render_cache_preparation(
          cloud,
          metrics_enabled: background_metrics_enabled,
          metric_logger: background_metric_logger,
          memory_fetcher: background_memory_fetcher
        )
      end

      ::UI.messagebox(
        "Импорт завершен за #{format('%.2f', duration)} сек. " \
        "Импортировано #{format_point_count(cloud.points.length)} из #{format_point_count(total_vertices)} точек"
      )
      result = Result.new(cloud, duration)
      @last_result = result
      result
    rescue StandardError => e
      @last_result = nil
      ::UI.messagebox("Ошибка импорта: #{e.message}")
      nil
    end

    def log_failure_details(job)
      Logger.debug do
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
      return unless cloud

      attempts = 0
      metrics_start = metrics_enabled ? Time.now : nil
      start_monotonic = monotonic_time
      timer_id = nil

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
          if timer_id
            ::UI.stop_timer(timer_id)
            timer_id = nil
          end
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
          if timer_id
            ::UI.stop_timer(timer_id)
            timer_id = nil
          end
          true
        else
          false
        end
      end

      completed = attempt.call
      return if completed

      unless defined?(::UI) && ::UI.respond_to?(:start_timer)
        Logger.debug('Таймеры UI недоступны, подготовка рендер-кэша остановлена досрочно')
        finalize.call(false)
        return
      end

      timer_id = ::UI.start_timer(RENDER_CACHE_RETRY_INTERVAL, repeat: true) do
        attempt.call
      end
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
