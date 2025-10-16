# frozen_string_literal: true

require 'sketchup.rb'

require_relative 'settings'
require_relative 'manager'
require_relative 'ui/commands'
require_relative 'ui/first_import_wizard'

module PointCloudImporter
  module Extension
    extend self

    def activate
      PointCloudImporter::Settings.instance.load!
      manager = PointCloudImporter::Manager.instance
      PointCloudImporter::UI::Commands.instance(manager).register!
      PointCloudImporter::UI::FirstImportWizard.show_if_needed(manager)
    end

    activate
  end
end
