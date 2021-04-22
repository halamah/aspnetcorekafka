using AspNetCore.Kafka.Utility;
using FluentAssertions;
using Xunit;

namespace Tests
{
    public class ConfigTests
    {
        [Theory]
        [InlineData("  function  (  true  )  =>   result  ")]
        [InlineData(" key : value ; ")]
        [InlineData("key : value")]
        [InlineData("key: value")]
        [InlineData("key: value;")]
        [InlineData("key:value,")]
        [InlineData("key= value")]
        [InlineData("key = value")]
        [InlineData("key=value")]
        [InlineData("function(10)")]
        [InlineData("function(10, 45)")]
        [InlineData("function(10, 45, internalValue)")]
        [InlineData("key: value, function(10, 45, internalValue)")]
        [InlineData("key: value; function(10)")]
        [InlineData("key: value ; function(false)")]
        [InlineData("key: value , function(false) => result")]
        [InlineData("function(false)=>result")]
        [InlineData("nested.name(nested.name, 10)=>result;")]
        [InlineData("nested.name: nested.value")]
        [InlineData("nested.name: nested.value, function()")]
        [InlineData("nested_nested.name: nested.value_2, function(value_1)")]
        public void ConfigValidation(string config) => config.ValidateConfigString().Should().BeTrue();

        [Fact]
        public void ConfigValues()
        {
            var result = new InlineConfigurationValues();
            
            result = "nested.func(nested.func.value);".ReadInlineConfiguration();
            result.Functions.Should().ContainKey("nested.func");
            result.Functions["nested.func"].Should().Contain("nested.func.value");
            result.Properties.Should().BeEmpty();
            result.Result.Should().BeNullOrEmpty();
            
            result = "nested.func(nested.func.value1, nested.func.value2);".ReadInlineConfiguration();
            result.Functions.Should().ContainKey("nested.func");
            result.Functions["nested.func"].Should().Contain("nested.func.value1");
            result.Functions["nested.func"].Should().Contain("nested.func.value2");
            result.Properties.Should().BeEmpty();
            result.Result.Should().BeNullOrEmpty();
            
            result = "nested.name: nested.value".ReadInlineConfiguration();
            result.Properties.Should().ContainKey("nested.name");
            result.Properties.Should().ContainValue("nested.value");
            result.Functions.Should().BeEmpty();
            
            result.Result.Should().BeNullOrEmpty();
            result = "nested.name: nested.value;".ReadInlineConfiguration();
            result.Properties.Should().ContainKey("nested.name");
            result.Properties.Should().ContainValue("nested.value");
            result.Functions.Should().BeEmpty();
            result.Result.Should().BeNullOrEmpty();
            
            result = " nested.name: nested.value , function (range ,10, 20), empty();".ReadInlineConfiguration();
            result.Properties.Should().ContainKey("nested.name");
            result.Properties.Should().ContainValue("nested.value");
            result.Functions.Should().ContainKey("function");
            result.Functions.Should().ContainKey("empty");
            result.Functions["function"].Should().Contain("range");
            result.Functions["function"].Should().Contain("10");
            result.Functions["function"].Should().Contain("20");
            result.Functions["empty"].Should().BeEmpty();
            result.Result.Should().BeNullOrEmpty();
            
            result = " nested.name: nested.value , function (range ,10, 20), empty()  => result_10;".ReadInlineConfiguration();
            result.Result.Should().Be("result_10");
        }
    }
}