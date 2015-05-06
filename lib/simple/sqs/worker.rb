require 'thread/pool'
require 'thread/channel'

require 'aws-sdk'

Thread.abort_on_exception = true

module Simple
  module Sqs
    class Worker
      def initialize(options)
        @options = {
          limit: 10,
          wait: false,
          poller_size: 1,
          idle_timeout: 20,
          wait_time_seconds: 20,
        }.merge(options)

        @pool   = Thread.pool 1, @options[:worker_size]
        @poller = build_poller
      end

      def execute(&block)
        subscribe_to_messages(&block)
        trap_signals

        loop do
          sleep 1
        end
      end

      private

      def build_poller
        response = Aws::SQS::Client.new.get_queue_url(queue_name: @options[:queue])

        poller_options = {
          skip_delete: true,
          max_number_of_messages: @options[:limit],
          wait_time_seconds:      @options[:wait_time_seconds],
          idle_timeout:           @options[:idle_timeout]
        }

        Aws::SQS::QueuePoller.new(response.first[:queue_url], poller_options)
      end

      def subscribe_to_messages(&block)
        @poller_threads = @options[:poller_size].times.map do
          Thread.new do
            loop do
              @poller.poll do |messages|
                handle messages, &block
              end
            end
          end
        end
      end

      def handle(messages, &block)
        messages.each do |message|
          break if @shutdown

          @pool.process do
            begin
              unless @shutdown
                block[message]

                @poller.delete_messages [message]
              end
            rescue StandardError => e
             p e.message
             raise e
           end
          end
        end

        @pool.wait if @options[:wait]
      end

      def trap_signals
        trap 'TTIN' do
          puts "   Thread count: #{Thread.list.count}"

          puts "   Thread inspections:"
          Thread.list.each do |thread|
            puts "    #{thread.object_id}: #{thread.status}"
          end

          puts "   GC stats:"
          puts GC.stat

          puts "   Object Space:"

          counts = Hash.new{ 0 }
          ObjectSpace.each_object do |o|
            counts[o.class] += 1
          end

          puts counts
        end

        %w(SIGTERM INT).each do |signal|
          trap signal do
            puts "Receive #{signal} signal. Shutting down pool..."

            Thread.new do
              @shutdown = true

              @pool.shutdown
              (@poller_threads || []).map(&:exit)

              puts "All done. Exit..."
              Process.exit(0)
            end
          end
        end
      end
    end
  end
end
