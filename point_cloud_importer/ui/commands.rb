# frozen_string_literal: true

require 'singleton'

require_relative '../importer'
require_relative '../ui/manager_dialog'
require_relative '../ui/first_import_wizard'
require_relative '../ui/measurement_history_dialog'

module PointCloudImporter
  module UI
    # Registers SketchUp UI commands.
    class Commands
      include Singleton

      def initialize
        @toolbar = nil
        @commands = {}
        @manager = nil
      end

      def self.instance(manager)
        inst = super()
        inst.manager = manager
        inst
      end

      attr_writer :manager

      def register!
        ensure_commands!
        add_menu_items
        add_help_menu_items
        add_toolbar
      end

      private

      def ensure_commands!
        return unless @commands.empty?

        importer = Importer.new(@manager)
        @commands[:import] = ::UI::Command.new('Импорт облака точек...') do
          importer.import_from_dialog
        end
        @commands[:import].tooltip = 'Импортировать облако точек из PLY файла'

        @commands[:toggle_visibility] = ::UI::Command.new('Показать/скрыть активное облако') do
          @manager.toggle_active_visibility
        end
        @commands[:toggle_visibility].tooltip = 'Переключить видимость активного облака'

        @commands[:toggle_inference] = ::UI::Command.new('Магнит к точкам (вкл/выкл)') do
          @manager.toggle_active_inference_guides
        end
        @commands[:toggle_inference].tooltip = 'Создать или удалить направляющие точки для привязки инструментов SketchUp'

        @commands[:measurement_tool] = ::UI::Command.new('Измерить расстояние в облаке') do
          Sketchup.active_model.select_tool(@manager.measurement_tool)
        end
        @commands[:measurement_tool].tooltip = 'Интерактивное измерение между точками'

        @commands[:measurement_history] = ::UI::Command.new('История измерений...') do
          UI::MeasurementHistoryDialog.new(@manager).show
        end
        @commands[:measurement_history].tooltip = 'Просмотреть последние измерения'

        @commands[:manage] = ::UI::Command.new('Менеджер облаков...') do
          UI::ManagerDialog.new(@manager).show
        end
        @commands[:manage].tooltip = 'Настроить облака, плотность отображения и размеры точек'

        @commands[:tutorial] = ::UI::Command.new('Point Cloud Tutorial') do
          UI::FirstImportWizard.show(@manager)
        end
        @commands[:tutorial].tooltip = 'Повторно открыть пошаговое обучение'
      end

      def add_menu_items
        menu = ::UI.menu('Extensions').add_submenu('Point Cloud Importer')
        menu.add_item(@commands[:import])
        menu.add_item(@commands[:manage])
        menu.add_separator
        menu.add_item(@commands[:toggle_visibility])
        menu.add_item(@commands[:toggle_inference])
        menu.add_item(@commands[:measurement_tool])
        menu.add_item(@commands[:measurement_history])
      end

      def add_help_menu_items
        menu = ::UI.menu('Help')
        menu.add_separator
        menu.add_item(@commands[:tutorial])
      end

      def add_toolbar
        return if @toolbar

        @toolbar = ::UI::Toolbar.new('Point Cloud Importer')
        @toolbar.add_item(@commands[:import])
        @toolbar.add_item(@commands[:toggle_visibility])
        @toolbar.add_item(@commands[:toggle_inference])
        @toolbar.add_item(@commands[:measurement_tool])
        @toolbar.add_item(@commands[:measurement_history])
        @toolbar.add_item(@commands[:manage])
        @toolbar.show
      end
    end
  end
end
