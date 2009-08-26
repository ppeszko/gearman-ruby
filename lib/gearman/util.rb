#!/usr/bin/env ruby

require 'socket'
require 'time'

module Gearman

  class ServerDownException < Exception; end

  # = Util
  #
  # == Description
  # Static helper methods and data used by other classes.
  class Util

    @@debug = false

    ##
    # Enable or disable debugging output (off by default).
    #
    # @param v  print debugging output
    def Util.debug=(v)
      @@debug = v
    end

    ##
    # Log a message if debugging is enabled.
    #
    # @param str  message to log
    def Util.log(str, force=false)
      puts "#{Time.now.strftime '%Y-%m-%d %H:%M:%S'} #{str}" if force or @@debug
    end

    ##
    # Log a message no matter what.
    #
    # @param str  message to log
    def Util.err(str)
      log(str, true)
    end

    def Util.ability_name_with_prefix(prefix,name)
      "#{prefix}\t#{name}"
    end

    class << self
      alias :ability_name_for_perl :ability_name_with_prefix
    end

  end

end
