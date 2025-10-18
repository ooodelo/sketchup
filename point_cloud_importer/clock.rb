# encoding: utf-8
# frozen_string_literal: true

module PointCloudImporter
  # Provides access to monotonic and wall clock time sources.
  module Clock
    module_function

    def monotonic
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    rescue NameError, Errno::EINVAL
      Process.clock_gettime(:float_second)
    rescue NameError, ArgumentError, Errno::EINVAL
      Process.clock_gettime(Process::CLOCK_REALTIME)
    rescue NameError, Errno::EINVAL
      Time.now.to_f
    end

    def now
      Time.now
    rescue StandardError
      Time.at(monotonic)
    end
  end
end
