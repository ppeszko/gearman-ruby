module Gearman
  module Evented

    module ClientReactor
      include Gearman::Evented::Reactor

      def client
        @callback_handler
      end

      def connection_completed
        @cbs_job_created ||= []
        super
      end

      def receive_data(data)
        Gearman::Protocol.decode_response(data).each do |type, handle, *data|
          dispatch_packet(type, handle, *data)
        end
      end

      def dispatch_packet_callback(&callback)
        @dispatch_packet_callback = callback
      end

      def dispatch_packet(type, handle, *data)
        log "Got #{type} with handle #{handle} from #{server} #{data}"
        if type == :job_created
          if cb = @cbs_job_created.shift
            cb.call(handle)
          end
        else
          log "Got #{type.to_s}, #{handle}, #{data} from #{server}"
          @dispatch_packet_callback.call(type, handle, data)
        end
      end

      def submit_job(task, &cb_job_created)
        cmd = "submit_job"
        cmd << "_#{task.priority}" if [ :high, :low ].include?(task.priority)
        cmd << "_bg" if task.background

        log "#{cmd} #{task.name}, #{task.payload} to #{server}"
        send cmd.to_sym, [ task.name, task.hash, task.payload ].join("\0")
        @cbs_job_created << cb_job_created if cb_job_created
      end
    end

  end
end
