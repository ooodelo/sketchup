# frozen_string_literal: true

require 'singleton'

require_relative '../importer'
require_relative '../ui/manager_dialog'

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

        @commands[:measurement_tool] = ::UI::Command.new('Измерить расстояние в облаке') do
          Sketchup.active_model.select_tool(@manager.measurement_tool)
        end
        @commands[:measurement_tool].tooltip = 'Интерактивное измерение между точками'

        @commands[:manage] = ::UI::Command.new('Менеджер облаков...') do
          UI::ManagerDialog.new(@manager).show
        end
        @commands[:manage].tooltip = 'Настроить облака, плотность отображения и размеры точек'
      end

      def add_menu_items
        menu = ::UI.menu('Extensions').add_submenu('Point Cloud Importer')
        menu.add_item(@commands[:import])
        menu.add_item(@commands[:manage])
        menu.add_separator
        menu.add_item(@commands[:toggle_visibility])
        menu.add_item(@commands[:measurement_tool])
      end

      def add_toolbar
        return if @toolbar

        @toolbar = ::UI::Toolbar.new('Point Cloud Importer')
        @toolbar.add_item(@commands[:import])
        @toolbar.add_item(@commands[:toggle_visibility])
        @toolbar.add_item(@commands[:measurement_tool])
        @toolbar.add_item(@commands[:manage])
        @toolbar.show
      end
    end
  end
end
