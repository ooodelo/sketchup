# encoding: utf-8
# frozen_string_literal: true

require 'singleton'

require_relative '../importer'
require_relative '../ui/manager_dialog'
require_relative '../ui/manager_panel'
require_relative '../ui/first_import_wizard'
require_relative '../ui/measurement_history_dialog'
require_relative '../stress_tester'

module PointCloudImporter
  module UI
    # Registers SketchUp UI commands.
    class Commands
      include Singleton

      def initialize
        @toolbar = nil
        @commands = {}
        @manager = nil
        @panel = nil
      end

      def self.instance(manager)
        inst = super()
        inst.manager = manager
        inst
      end

      attr_writer :manager

      def command(key)
        ensure_commands!
        @commands[key.to_sym]
      end

      def register!
        ensure_commands!
        add_menu_items
        add_help_menu_items
        add_toolbar
      end

      def refresh_panel
        return unless @panel
        return unless @panel.visible?

        @panel.refresh!
      end
      alias_method :refresh_panel_if_visible, :refresh_panel
      public :refresh_panel_if_visible

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

        @commands[:stress_test] = ::UI::Command.new('Стресс-тест импорта') do
          StressTester.new(@manager).run
        end
        @commands[:stress_test].tooltip = 'Запустить эталонный импорт с записью телеметрии'

        @commands[:manage] = ::UI::Command.new('Менеджер облаков...') do
          UI::ManagerDialog.new(@manager).show
        end
        @commands[:manage].tooltip = 'Настроить облака, плотность отображения и размеры точек'

        @commands[:panel] = ::UI::Command.new('Point Cloud Manager') do
          panel.show
        end
        @commands[:panel].tooltip = 'Открыть компактную панель управления облаками'
        icon16 = icon_path('cloud_16.png')
        icon24 = icon_path('cloud_24.png')
        @commands[:panel].small_icon = icon16 if File.exist?(icon16)
        @commands[:panel].large_icon = icon24 if File.exist?(icon24)

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
        menu.add_separator
        menu.add_item(@commands[:stress_test])
      end

      def add_help_menu_items
        menu = ::UI.menu('Help')
        menu.add_separator
        menu.add_item(@commands[:tutorial])
      end

      def add_toolbar
        return if @toolbar

        @toolbar = ::UI::Toolbar.new('Point Cloud Importer')
        @toolbar.add_item(@commands[:panel])
        @toolbar.show
      end

      def panel
        @panel ||= UI::ManagerPanel.new(@manager, self)
      end

      def icon_path(name)
        File.expand_path(File.join('icons', name), __dir__)
      end

    end
  end
end
