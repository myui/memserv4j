require 'dalli'

dc = Dalli::Client.new('192.168.142.132:11211')
p dc.set('abc', 123)
p dc.get('abc')
p dc.get('abc')
