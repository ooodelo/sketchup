# encoding: utf-8
# frozen_string_literal: true

require 'minitest/autorun'

class EncodingHeaderTest < Minitest::Test
  def test_ruby_files_have_encoding_header
    ruby_files = Dir.glob('**/*.rb').reject { |path| path.start_with?('vendor/') }
    ruby_files.each do |path|
      next unless File.file?(path)

      first_lines = File.readlines(path, encoding: 'UTF-8')[0, 2].map(&:chomp)
      assert_includes first_lines, '# encoding: utf-8', "#{path} должен начинаться с '# encoding: utf-8'"
      assert_includes first_lines, '# frozen_string_literal: true', "#{path} должен содержать директиву '# frozen_string_literal: true'"
      assert first_lines.index('# encoding: utf-8') < first_lines.index('# frozen_string_literal: true'),
             "В файле #{path} строка '# encoding: utf-8' должна предшествовать '# frozen_string_literal: true'"
    end
  end
end
