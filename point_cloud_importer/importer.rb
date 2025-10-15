# frozen_string_literal: true

require 'stringio'

require_relative 'settings'
require_relative 'point_cloud'
require_relative 'import_job'

module PointCloudImporter
  # High-level importer orchestrating parsing and creation of point clouds.
  class Importer
    Result = Struct.new(:cloud, :duration)

    def initialize(manager)
      @manager = manager
    end

    def import_from_dialog
      path = UI.openpanel('Выберите файл облака точек', nil, 'PLY (*.ply)|*.ply||')
      return unless path

      options = prompt_options
      return unless options

      import(path, options)
    end

    def import(path, options = {})
      job = ImportJob.new(path, options).start
      dialog = build_progress_dialog(job)

      result = nil
      timer_id = nil

      update_progress_dialog(dialog, job.progress, job.message)

      timer_id = UI.start_timer(0.1, true) do
        case job.status
        when :running
          update_progress_dialog(dialog, job.progress, job.message)
        when :success
          UI.stop_timer(timer_id)
          update_progress_dialog(dialog, job.progress, job.message)
          dialog.close
          result = finalize_success(job, options)
        when :cancelled
          UI.stop_timer(timer_id)
          dialog.close
          UI.messagebox('Импорт отменен пользователем.')
        when :failed
          UI.stop_timer(timer_id)
          dialog.close
          handle_failure(job.error)
        end
      end

      dialog.show_modal
      job.wait
      result
    ensure
      UI.stop_timer(timer_id) if timer_id
      if dialog && dialog.respond_to?(:visible?) && dialog.visible?
        dialog.close
      end
    end

    STYLE_LABELS = {
      square: 'квадрат',
      round: 'круг',
      plus: 'крест'
    }.freeze

    private

    def prompt_options
      settings = Settings.instance
      prompts = ['Размер точки (1-10)', 'Стиль точки', 'Доля отображаемых точек (0.01-1.0)', 'Максимум точек в кадре (10000-8000000)']
      defaults = [
        settings[:point_size],
        display_name(settings[:point_style]),
        settings[:density],
        settings[:max_display_points]
      ]
      style_list = available_style_labels.join('|')
      lists = ['', style_list, '', '']
      input = UI.inputbox(prompts, defaults, lists, 'Настройки визуализации')
      return unless input

      point_size, style_label, density, max_points = input
      {
        point_size: point_size.to_i,
        point_style: resolve_style(style_label),
        display_density: density.to_f,
        max_display_points: max_points.to_i
      }
    end

    def finalize_success(job, options)
      data = job.result || {}
      points = data[:points] || []
      colors = data[:colors]
      metadata = data[:metadata]

      name = File.basename(job.path, '.*')
      cloud = PointCloud.new(name: name, points: points, colors: colors, metadata: metadata)
      cloud.density = options[:display_density] if options[:display_density]
      cloud.point_size = options[:point_size] if options[:point_size]
      cloud.point_style = options[:point_style] if options[:point_style]
      cloud.max_display_points = options[:max_display_points] if options[:max_display_points]

      @manager.add_cloud(cloud)
      duration = job.duration || 0.0
      UI.messagebox("Импорт завершен за #{format('%.2f', duration)} сек. Точек: #{points.length}")

      Result.new(cloud, duration)
    rescue PlyParser::UnsupportedFormat => e
      UI.messagebox("Формат не поддерживается: #{e.message}")
      nil
    rescue StandardError => e
      UI.messagebox("Ошибка импорта: #{e.message}")
      nil
    end

    def handle_failure(error)
      case error
      when PlyParser::UnsupportedFormat
        UI.messagebox("Формат не поддерживается: #{error.message}")
      when nil
        UI.messagebox('Ошибка импорта: неизвестная ошибка.')
      else
        UI.messagebox("Ошибка импорта: #{error.message}")
      end
    end

    def build_progress_dialog(job)
      dialog = UI::HtmlDialog.new(
        dialog_title: 'Импорт облака точек',
        preferences_key: 'PointCloudImporter/ImportProgress',
        resizable: false,
        width: 360,
        height: 180,
        style: UI::HtmlDialog::STYLE_DIALOG
      )

      dialog.set_html(progress_dialog_html)
      dialog.add_action_callback('cancel_import') { job.cancel! }
      dialog.set_can_close do
        job.cancel!
        true
      end
      dialog
    end

    def progress_dialog_html
      <<~HTML
        <!DOCTYPE html>
        <html>
          <head>
            <meta charset="utf-8">
            <title>Импорт облака точек</title>
            <style>
              body {
                font-family: "Segoe UI", Helvetica, Arial, sans-serif;
                margin: 16px;
                color: #222;
              }
              h1 {
                font-size: 16px;
                margin: 0 0 12px;
              }
              #progress-container {
                width: 100%;
                height: 18px;
                border: 1px solid #999;
                border-radius: 4px;
                overflow: hidden;
                background: #f2f2f2;
                margin-bottom: 12px;
              }
              #progress-bar {
                height: 100%;
                width: 0%;
                background: #4a90e2;
                transition: width 0.1s ease-out;
              }
              #status {
                font-size: 13px;
                margin-bottom: 12px;
              }
              button {
                padding: 6px 12px;
                font-size: 13px;
              }
            </style>
          </head>
          <body>
            <h1>Импорт облака точек</h1>
            <div id="progress-container">
              <div id="progress-bar"></div>
            </div>
            <div id="status">Подготовка...</div>
            <button id="cancel">Отмена</button>
            <script>
              window.updateProgress = function(percent, message) {
                var progress = Math.max(0, Math.min(percent, 100));
                document.getElementById('progress-bar').style.width = progress + '%';
                document.getElementById('status').textContent = message || '';
              };

              (function() {
                var cancelButton = document.getElementById('cancel');
                cancelButton.addEventListener('click', function() {
                  if (window.sketchup && window.sketchup.cancel_import) {
                    window.sketchup.cancel_import();
                    cancelButton.disabled = true;
                  }
                });
              })();
            </script>
          </body>
        </html>
      HTML
    end

    def update_progress_dialog(dialog, progress, message)
      percent = (progress.to_f * 100.0).clamp(0.0, 100.0)
      js_message = js_string(message.to_s.empty? ? '...' : message.to_s)
      dialog.execute_script("updateProgress(#{format('%.2f', percent)}, #{js_message})")
    rescue StandardError
      # Ignore dialog update errors (dialog may be closing).
    end

    def js_string(value)
      escaped = value.gsub('\\', '\\\\').gsub("'", "\\'").gsub("\n", '\\n')
      "'#{escaped}'"
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

  end
end
