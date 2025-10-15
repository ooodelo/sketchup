# frozen_string_literal: true

require 'sketchup.rb'
require 'extensions.rb'

module PointCloudImporter
  EXTENSION_NAME = 'Point Cloud Importer'.freeze
  EXTENSION_PATH = File.join(File.dirname(__FILE__), 'point_cloud_importer', 'extension').freeze

  unless file_loaded?(__FILE__)
    extension = SketchupExtension.new(EXTENSION_NAME, EXTENSION_PATH)
    extension.version     = '1.0.0'
    extension.description = 'High-performance point cloud importer with visualization and measurement tools.'
    extension.creator     = 'PointCloudImporter Team'

    Sketchup.register_extension(extension, true)
    file_loaded(__FILE__)
  end
end
