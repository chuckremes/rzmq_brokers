
class DBMessage
  def self.create_from(worker, message)
    if message.request?
      LookupRequest.from_message(worker, message)
    elsif message.success_reply?
      LookupReplySuccess.from_message(message)
    elsif message.failure_reply?
      LookupReplyFailure.from_message(message)
    end
  end

  def self.decode_payload(payload_strings)
    if payload_strings
      payload_strings.map { |string| JSON.parse(string) }
    else
      Array.new(1) { Hash.new }
    end
  end

  def self.encode_payload(hsh)
    JSON.generate(hsh)
  end

  def self.create_accessors(mod, fields)
    fields.each do |field_name|
      code = <<-code
      def #{ field_name } (value = nil)
        if value
          @#{field_name} = value
        else
          @#{field_name}
        end
      end

      def #{ field_name }=(value)
        @#{field_name} = value
      end
      code

      mod.class_eval code
    end
  end

  # the Majordomo messages expect all payloads to be wrapped in
  # arrays; each element of the array will be sent as a separate
  # frame
  def encode(string)
    [string]
  end
end # DBMessage


class LookupRequest < DBMessage
  create_accessors(self, %w(worker sequence_id service_name contract_id range_start range_end duration))

  def self.from_message(worker, message)
    payloads = decode_payload(message.payload)
    payload = payloads[0] # only care about first frame

    new do
      worker worker
      sequence_id message.sequence_id
      range_start payload['range_start']
      range_end payload['range_end']
      duration payload['duration']
      contract_id payload['contract_id']
    end
  end

  def initialize(&blk)
    instance_eval(&blk) if block_given?
  end

  def encode
    string = LookupRequest.encode_payload({
      'range_start' => range_start,
      'range_end' => range_end,
      'duration' => duration,
      'contract_id' => contract_id
    })

    super(string)
  end
end # LookupRequest


class LookupReplySuccess < DBMessage
  create_accessors(self, %w(sequence_id answer))

  def self.from_request(request)
    new do
      sequence_id request.sequence_id
    end
  end

  def self.from_message(message)
    payloads = decode_payload(message.payload)
    payload = payloads[0] # only care about first frame

    new do
      sequence_id message.sequence_id
      answer payload['answer']
    end
  end

  def initialize(&blk)
    instance_eval(&blk) if block_given?
  end

  def encode
    string = LookupReplySuccess.encode_payload({
      'answer' => answer
    })

    super(string)
  end
end # LookupReplySuccess


class LookupReplyFailure < LookupReplySuccess
end
