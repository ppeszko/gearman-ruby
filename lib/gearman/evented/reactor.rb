module Gearman
  module Evented

    module Reactor
      include EM::Deferrable

      def connect(reactor, host, port, opts = {})
        conn = EM.connect(host, (port || 4730), reactor) do |c|
          c.instance_eval do
            @host = host
            @port = port || 4730
            @opts = opts
            @connection ||= conn
          end
        end
        @connection ||= conn
      end

      def connected?
        @connected
      end

      def server
        @hostport ||= [ @host, @port ].join(":")
      end

      def connection_completed
        log "connected #{@connection}"
        @connected = true
        @reconnecting = false
      end

      def unbind
        log 'disconnected'
        @connected = false
        EM.next_tick { reconnect }
      end

      def reconnect
        if @reconnecting
          EM.add_timer(@opts[:reconnect_sec] || 30) { reconnect }
          return
        else
          @reconnecting = true
          @deferred_status = nil
        end

        log 'reconnecting'
        EM.reconnect(@host, @port.to_i, self)
      end

      def send(command, data = nil)
        log "[#{self}] Send #{command} on connection #{@connection}"
        send_data(Gearman::Protocol.encode_request(command, data))
      end

      def log(msg)
        Gearman::Util.log msg
      end
    end

  end
end
