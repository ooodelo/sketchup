# frozen_string_literal: true

require 'stringio'

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

    def initialize(manager)
      @manager = manager
      @last_result = nil
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
      Logger.debug do
        options_text = job.options.map { |key, value| "#{key}=#{value.inspect}" }.join(', ')
        formatted_options = options_text.empty? ? 'без опций' : options_text
        "Запуск импорта файла #{job.path.inspect} (#{formatted_options})"
      end

      progress_dialog = UI::ImportProgressDialog.new(job) { job.cancel! }
      progress_dialog.show

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
          chunk_size = job.options[:chunk_size] || settings[:import_chunk_size] || PlyParser::DEFAULT_CHUNK_SIZE
          chunk_size = chunk_size.to_i
          chunk_size = 1 if chunk_size < 1
          invalidate_every_n_chunks = job.options[:invalidate_every_n_chunks] || settings[:invalidate_every_n_chunks] || 1
          invalidate_every_n_chunks = invalidate_every_n_chunks.to_i
          invalidate_every_n_chunks = 1 if invalidate_every_n_chunks < 1

          cloud = PointCloud.new(name: name, metadata: parser.metadata || {})
          job.cloud = cloud

          chunks_processed = 0
          metadata = parser.parse(chunk_size: chunk_size) do |points_chunk, colors_chunk, intensities_chunk, processed|
            next if job.cancel_requested?
            next unless points_chunk && !points_chunk.empty?

            cloud.append_points!(points_chunk, colors_chunk, intensities_chunk)

            chunks_processed += 1

            total_vertices = parser.total_vertex_count.to_i
            total_vertices = processed if total_vertices.zero? || total_vertices < processed
            job.update_progress(
              processed_vertices: processed,
              total_vertices: total_vertices,
              message: format_progress_message(processed, total_vertices)
            )

            next unless (chunks_processed % invalidate_every_n_chunks).zero?

            @manager.view&.invalidate
          end

          cloud.finalize_bounds!

          if job.cancel_requested?
            job.mark_cancelled!
            next
          end

          metadata ||= parser.metadata
          cloud.update_metadata!(metadata)

          total_vertices = parser.total_vertex_count.to_i
          total_vertices = cloud.points.length if total_vertices.zero?

          job.complete!(
            cloud: cloud,
            metadata: metadata,
            duration: Time.now - start_time,
            total_vertices: total_vertices
          )
        rescue PlyParser::Cancelled
          job.mark_cancelled!
        rescue PlyParser::UnsupportedFormat => e
          job.fail!(e)
        rescue StandardError => e
          job.fail!(e)
        end
      end

      job.thread = worker

      timer_id = nil
      timer_id = ::UI.start_timer(0.1, repeat: true) do
        progress_dialog.update
        next unless job.finished?

        ::UI.stop_timer(timer_id) if timer_id
        progress_dialog.close
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

      Thread.new do
        begin
          cloud.prepare_render_cache!
          @manager.view&.invalidate
        rescue StandardError
          # Ignore background cache errors
        end
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
