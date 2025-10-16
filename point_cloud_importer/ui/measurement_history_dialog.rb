# encoding: utf-8
# frozen_string_literal: true

require 'json'
require_relative '../measurement_history'

module PointCloudImporter
  module UI
    # HtmlDialog wrapper for displaying recent measurements.
    class MeasurementHistoryDialog
      TEMPLATE = File.expand_path('measurement_history_dialog.html', __dir__)

      def initialize(manager)
        @manager = manager
        @history = manager.measurement_history
        @dialog = ::UI::HtmlDialog.new(
          dialog_title: 'Measurement History',
          preferences_key: 'PointCloudImporter::MeasurementHistoryDialog',
          scrollable: true,
          resizable: true,
          width: 500,
          height: 420
        )
        register_callbacks
        html = File.read(TEMPLATE, encoding: 'UTF-8')
        @dialog.set_html(html)
        attach_history_listener
      end

      def show
        refresh
        @dialog.show
      end

      private

      def register_callbacks
        @dialog.add_action_callback('pci_history_ready') { refresh }
        @dialog.add_action_callback('pci_history_preview') do |_, index|
          toggle_preview(index)
        end
        @dialog.add_action_callback('pci_history_copy') do |_, index|
          copy_measurement(index)
        end
        @dialog.add_action_callback('pci_history_delete') do |_, index|
          delete_measurement(index)
        end
        @dialog.add_action_callback('pci_history_export') { export_csv }
        @dialog.add_action_callback('pci_history_dimension') do |_, index|
          create_dimension(index)
        end
      end

      def attach_history_listener
        return unless @history

        @listener = proc { refresh_if_visible }
        @history.add_listener(@listener)
        @dialog.set_on_closed do
          @history.remove_listener(@listener) if @listener
          @history.preview_index = nil if @history.preview_index
          @listener = nil
        end
      end

      def refresh_if_visible
        refresh if @dialog.visible?
      end

      def refresh
        return unless @history

        payload = {
          entries: serialize_entries(@history.entries),
          preview_index: @history.preview_index
        }
        @dialog.execute_script("window.pciHistory && window.pciHistory.update(#{JSON.generate(payload)});")
      end

      def serialize_entries(entries)
        entries.each_with_index.map do |entry, index|
          {
            index: index,
            label: entry[:label],
            distance: entry[:distance],
            timestamp: entry[:timestamp].iso8601,
            formatted_time: entry[:timestamp].strftime('%d.%m.%Y %H:%M:%S'),
            from: entry[:from].to_a,
            to: entry[:to].to_a
          }
        end
      end

      def toggle_preview(index)
        return unless @history

        idx = begin
          Integer(index)
        rescue ArgumentError, TypeError
          nil
        end
        return unless idx

        if @history.preview_index == idx
          @history.preview_index = nil
        else
          @history.preview_index = idx
        end
      end

      def copy_measurement(index)
        entry = @history&.entry_at(index)
        return unless entry

        distance = entry[:label].to_s.empty? ? Sketchup.format_length(entry[:distance]) : entry[:label]
        formatted = [
          distance,
          entry[:timestamp].strftime('%Y-%m-%d %H:%M:%S'),
          "От: #{format_point(entry[:from])}",
          "До: #{format_point(entry[:to])}"
        ].join(' | ')
        ::UI.copy_to_clipboard(formatted)
      end

      def delete_measurement(index)
        return unless @history

        @history.remove(index)
      end

      def export_csv
        return unless @history

        path = ::UI.savepanel('Export Measurement History', nil, 'measurements.csv')
        return unless path

        begin
          File.write(path, @history.to_csv)
        rescue SystemCallError => e
          ::UI.messagebox("Не удалось сохранить CSV: #{e.message}")
        end
      end

      def create_dimension(index)
        entry = @history&.entry_at(index)
        return unless entry

        dimension = @manager.save_measurement_as_dimension(entry)
        unless dimension
          ::UI.messagebox('Не удалось создать размер для измерения.')
        end
      end

      def format_point(point)
        components = point.to_a.map { |value| format('%.3f', value) }
        "(#{components.join(', ')})"
      end
    end
  end
end
