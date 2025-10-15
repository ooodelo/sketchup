# frozen_string_literal: true

require 'stringio'

require_relative 'settings'
require_relative 'point_cloud'
require_relative 'ply_parser'
require_relative 'import_job'
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
      path = UI.openpanel('Выберите файл облака точек', nil, 'PLY (*.ply)|*.ply||')
      return unless path

      options = prompt_options
      return unless options

      import(path, options)
    end

    def import(path, options = {})
      options = { import_step: 1 }.merge(options || {})
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
      progress_dialog = UI::ImportProgressDialog.new(job) { job.cancel! }
      progress_dialog.show

      worker = Thread.new do
        Thread.current.abort_on_exception = false
        begin
          job.start!
          job.update_progress(0.0, 'Чтение PLY...')
          start_time = Time.now
          parser = PlyParser.new(
            job.path,
            import_step: job.options[:import_step],
            progress_callback: job.progress_callback,
            cancelled_callback: -> { job.cancel_requested? }
          )
          points, colors, metadata = parser.parse
          if job.cancel_requested?
            job.mark_cancelled!
          else
            total_vertices = parser.total_vertex_count.to_i
            total_vertices = points.length if total_vertices.zero?
            job.complete!(
              points: points,
              colors: colors,
              metadata: metadata,
              duration: Time.now - start_time,
              total_vertices: total_vertices,
              import_step: parser.import_step
            )
          end
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
      timer_id = UI.start_timer(0.1, repeat: true) do
        progress_dialog.update
        next unless job.finished?

        UI.stop_timer(timer_id) if timer_id
        progress_dialog.close
        job.thread&.join
        finalize_job(job)
      end
    end

    def finalize_job(job)
      case job.status
      when :completed
        create_cloud(job)
      when :failed
        @last_result = nil
        UI.messagebox("Ошибка импорта: #{job.error.message}") if job.error
      when :cancelled
        @last_result = nil
        UI.messagebox('Импорт отменен пользователем.')
      else
        @last_result = nil
      end
    end

    def create_cloud(job)
      data = job.result
      points = data[:points]
      colors = data[:colors]
      metadata = data[:metadata]
      duration = data[:duration]
      total_vertices = data[:total_vertices]
      import_step = data[:import_step]

      raise ArgumentError, 'PLY файл не содержит точек' if points.nil? || points.empty?

      name = File.basename(job.path, '.*')
      cloud = PointCloud.new(name: name, points: points, colors: colors, metadata: metadata)
      apply_visual_options(cloud, job.options)

      @manager.add_cloud(cloud)
      UI.messagebox(
        "Импорт завершен за #{format('%.2f', duration)} сек. " \
        "Импортировано #{points.length} из #{total_vertices} точек (каждая #{import_step}-я)"
      )
      result = Result.new(cloud, duration)
      @last_result = result
      result
    rescue StandardError => e
      @last_result = nil
      UI.messagebox("Ошибка импорта: #{e.message}")
      nil
    end

    def apply_visual_options(cloud, options)
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
        'Максимум точек в кадре (10000-8000000)',
        'Импортировать каждую N-ю точку (1 = все точки)'
      ]
      defaults = [
        settings[:point_size],
        display_name(settings[:point_style]),
        settings[:density],
        settings[:max_display_points],
        1
      ]
      style_list = available_style_labels.join('|')
      lists = ['', style_list, '', '', '']
      input = UI.inputbox(prompts, defaults, lists, 'Настройки визуализации')
      return unless input

      point_size, style_label, density, max_points, import_step_input = input
      import_step = validate_import_step(import_step_input)
      return unless import_step
      {
        point_size: point_size.to_i,
        point_style: resolve_style(style_label),
        display_density: density.to_f,
        max_display_points: max_points.to_i,
        import_step: import_step
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

    def validate_import_step(value)
      step = Integer(value)
      if step < 1
        UI.messagebox('Шаг импорта должен быть целым числом 1 или больше.')
        return nil
      end
      step
    rescue ArgumentError, TypeError
      UI.messagebox('Шаг импорта должен быть целым числом 1 или больше.')
      nil
    end
  end
end
