begin
  require 'active_support/core_ext/hash/keys'
  require 'active_support/core_ext/hash/deep_merge'
rescue LoadError
  class Hash
    def stringify_keys
      each_key do |key|
        self[key.to_s] = delete(key)
      end
      self
    end unless {}.respond_to?(:stringify_keys)

    def symbolize_keys
      each_key do |key|
        self[(key.to_sym rescue key) || key] = delete(key)
      end
      self
    end unless {}.respond_to?(:symbolize_keys)

    def deep_symbolize_keys
      each_key do |key|
        value = delete(key)
        self[(key.to_sym rescue key) || key] = value

        value.deep_symbolize_keys if value.is_a? Hash
      end
      self
    end unless {}.respond_to?(:deep_symbolize_keys)
  end
end

begin
  require 'active_support/core_ext/string/inflections'
rescue LoadError
  class String
    def constantize
      names = split('::')
      names.shift if names.empty? || names.first.empty?

      constant = Object
      names.each do |name|
        constant = constant.const_defined?(name) ? constant.const_get(name) : constant.const_missing(name)
      end
      constant
    end
  end unless ''.respond_to?(:constantize)
end
