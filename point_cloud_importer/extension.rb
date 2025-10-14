# frozen_string_literal: true

require 'sketchup.rb'

require_relative 'settings'
require_relative 'manager'
require_relative 'ui/commands'

module PointCloudImporter
  module Extension
    extend self

    def activate
      PointCloudImporter::Settings.instance.load!
      manager = PointCloudImporter::Manager.instance
      PointCloudImporter::UI::Commands.instance(manager).register!
    end

    activate
  end
end
