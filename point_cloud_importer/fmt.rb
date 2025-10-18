# encoding: utf-8
# frozen_string_literal: true

require_relative 'numbers'

module PointCloudImporter
  # Human-friendly formatting helpers for diagnostics and UI.
  module Fmt
    module_function

    def n(value, suffix: '', zero: nil, precision_large: 1, precision_small: 0, positive_only: true, round_small: true)
      zero ||= default_zero(suffix)
      return zero if value.nil?

      number = value.to_f
      return zero unless number.finite?
      return zero if positive_only && number <= 0.0

      abs_value = number.abs
      if abs_value >= 1_000_000.0
        formatted = format("%.#{precision_large}f", number / 1_000_000.0)
        unit = 'M'
      elsif abs_value >= 1_000.0
        formatted = format("%.#{precision_large}f", number / 1_000.0)
        unit = 'k'
      else
        formatted = if precision_small.positive?
                      format("%.#{precision_small}f", number)
                    else
                      small = round_small ? number.round : number.to_i
                      small.to_i.to_s
                    end
        unit = ''
      end

      "#{formatted}#{unit}#{suffix}"
    rescue StandardError
      zero
    end

    def bytes(value, precision: 2)
      return '0 B' if value.nil?

      bytes = value.to_f
      return '0 B' unless bytes.finite?

      units = [
        ['GB', 1024.0**3],
        ['MB', 1024.0**2],
        ['KB', 1024.0],
        ['B', 1.0]
      ]

      abs_value = bytes.abs
      sign = bytes.negative? ? -1 : 1

      unit, divisor = units.detect { |_name, threshold| abs_value >= threshold } || units.last
      scaled = abs_value / divisor

      formatted = if divisor == 1.0
                    scaled.round.to_i.to_s
                  else
                    strip_trailing_zero(format("%.#{precision}f", scaled))
                  end

      sign_prefix = sign < 0 ? '-' : ''
      "#{sign_prefix}#{formatted} #{unit}"
    rescue StandardError
      '0 B'
    end

    def strip_trailing_zero(value)
      value.sub(/\.0+\z/, '').sub(/(\.\d*?)0+\z/, '\1')
    end
    private_class_method :strip_trailing_zero

    def default_zero(suffix)
      suffix.to_s.empty? ? '0' : "0#{suffix}"
    end
    private_class_method :default_zero
  end
end
