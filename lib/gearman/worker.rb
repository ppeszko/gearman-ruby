module Gearman
  class Worker

    attr_reader :abilities

    def initialize(job_servers, opts = {})
      @reactors = []
      @abilities = {}

      @job_servers = if job_servers.is_a?(String)
        [ job_servers ]
      else
        job_servers
      end

      @opts = opts
    end

    def add_ability(name, timeout = nil, &f)
      remove_ability(name) if @abilities.has_key?(name)
      @abilities[name] = f
      @reactors.each {|reactor| reactor.announce_ability(name, timeout) }
    end

    def remove_ability(name)
      @abilities.delete(name)
      @reactors.each {|reactor| reactor.announce_disability(name) }
    end

    def has_ability?(name)
      @abilities.has_key?(name)
    end

    def work
      EM.run do
        @job_servers.each do |hostport|
          host, port = hostport.split(":")
          reactor = Gearman::Evented::WorkerReactor.connect(host, port, self, @opts)
          reactor.callback do
            @abilities.keys.each do |ability|
              reactor.announce_ability(ability, nil)
            end
            reactor.grab_job do |handle, func, args|
              handle_job_assign(reactor, handle, func.to_s, args)
            end
          end
          @reactors << reactor
        end
      end
    end

    def handle_job_assign(reactor, handle, func, args = '')
      return unless handle
      unless func
        log "ERROR: Ignoring job_assign with no function"
        return
      end

      log "Got job_assign '#{func}' with handle #{handle} and #{args.size rescue 0} byte(s)"

      if !has_ability?(func)
        log "Ignoring job_assign for unsupported func #{func} with handle #{handle}"
        reactor.work_fail handle
        return
      end

      exception = nil
      begin
        ret = abilities[func].call(args, Gearman::Job.new(reactor, handle))
      rescue Exception => e
        exception = e
      end

      if ret && exception.nil?
        ret = ret.to_s
        log "Sending work_complete for #{handle} with #{ret.size} byte(s)"
        reactor.work_complete handle, ret
      elsif exception.nil?
        log "Sending work_fail for #{handle} to #{server}"
        reactor.work_fail handle
      elsif exception
        log "exception #{exception.message}, sending work_warning, work_fail for #{handle}"
        reactor.work_warning handle, exception.message
        reactor.work_fail handle
      end
    end

    private
      def log(msg)
        Gearman::Util.log msg
      end
  end
end
