#!/usr/bin/env ruby
# frozen_string_literal: true

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'rubyzip'

# Extract Flink CDC revision number from global pom.xml
begin
  REVISION_NUMBER = File.read('pom.xml').scan(%r{<revision>(.*)</revision>}).last[0]
rescue NoMethodError
  abort 'Could not extract Flink CDC revision number from pom.xml'
end

puts "Flink CDC version: '#{REVISION_NUMBER}'"

# These packages don't need to be checked
EXCLUDED_PACKAGES = %w[flink-cdc-dist flink-cdc-e2e-tests].freeze


# Traversing maven module in given path
def traverse_module(path)
  module_name = File.basename path
  return if EXCLUDED_PACKAGES.include?(module_name)

  jar_file = File.join path, 'target', "#{module_name}-#{REVISION_NUMBER}.jar"
  check_jar_license jar_file if File.exist? jar_file

  File.read(File.join(path, 'pom.xml')).scan(%r{<module>(.*)</module>}).map(&:first).each do |submodule|
    traverse_module File.join(path, submodule.to_s) unless submodule.nil?
  end
end

# Check license issues in given jar file
def check_jar_license(jar_file)
  puts "Checking jar file #{jar_file}"
end

traverse_module '.'
