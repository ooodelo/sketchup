# encoding: utf-8
# frozen_string_literal: true

module PointCloudImporter
  # Utility helpers for safe IO interactions.
  module IOUtils
    class AccessError < StandardError; end
    class NotFoundError < StandardError; end

    module_function

    def open_read(path)
      raise ArgumentError, 'Блок обязателен для open_read' unless block_given?

      File.open(path, 'rb') do |io|
        return yield(io)
      end
    rescue Errno::EACCES
      raise AccessError, "Нет доступа к файлу: #{path}"
    rescue Errno::ENOENT
      raise NotFoundError, "Файл не найден: #{path}"
    end
  end
end
