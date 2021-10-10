using System;
using System.Linq;
using AspNetCore.Kafka.Utility;
using FluentAssertions;
using Xunit;

namespace Tests
{
    public class ConfigurationTests
    {
        [Theory]
        [InlineData("Endpoint = http://postgraphile.kube.devbl/graphql, Login = ams_user; Password = MTbT2QLp8kssP9j4")]
        [InlineData("function(value1.amount + value2.amount)")]
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
        [InlineData("providerName: test_gateway, paymentMethodName: test_method_paygroup, nominalAmount.amount: 10 => 5.5")]
        [InlineData("providerName: test_gateway, paymentMethodName: test_method_paygroup, range(nominalAmount.amount, 10, 30) => 6.5")]
        public void ConfigValidation(string input)
        {
            ConfigurationString.Validate(input).Should().BeTrue();
            
            input.Invoking(ConfigurationString.Parse).Should().NotThrow();
        }

        [Fact]
        public void ConfigValues()
        {
            Having("nested.func(nested.func.value);",
                x => x.Functions.Should().ContainKey("nested.func"),
                x => x.Functions["nested.func"].Should().Contain("nested.func.value"),
                x => x.Properties.Should().BeEmpty(),
                x => x.Result.Should().BeNullOrEmpty());

            Having(
                "nested.func(nested.func.value1, nested.func.value2);",
                x => x.Functions.Should().ContainKey("nested.func"),
                x => x.Functions["nested.func"].Should().Contain("nested.func.value1"),
                x => x.Functions["nested.func"].Should().Contain("nested.func.value2"),
                x => x.Properties.Should().BeEmpty(),
                x => x.Result.Should().BeNullOrEmpty());
            
            Having(
                "nested.name: nested.value",
                x => x.Properties.Should().ContainKey("nested.name"),
                x => x.Properties.Should().ContainValue("nested.value"),
                x => x.Functions.Should().BeEmpty(),
                x => x.Result.Should().BeNullOrEmpty());
            
            Having(
                "nested.name: nested.value;",
                x => x.Properties.Should().ContainKey("nested.name"),
                x => x.Properties.Should().ContainValue("nested.value"),
                x => x.Functions.Should().BeEmpty(),
                x => x.Result.Should().BeNullOrEmpty());
            
            Having(
                " nested.name: nested.value , function (range ,10, 20), empty();",
                x => x.Properties.Should().ContainKey("nested.name"),
                x => x.Properties.Should().ContainValue("nested.value"),
                x => x.Functions.Should().ContainKey("function"),
                x => x.Functions.Should().ContainKey("empty"),
                x => x.Functions["function"].Should().Contain("range"),
                x => x.Functions["function"].Should().Contain("10"),
                x => x.Functions["function"].Should().Contain("20"),
                x => x.Functions["empty"].Should().BeEmpty(),
                x => x.Result.Should().BeNullOrEmpty());
            
            Having(
                " nested.name: nested.value , function (range ,10, 20), empty()  => result_10;",
                x => x.Result.Should().Be("result_10"));
            
            Having(
                " function ( value1.amount + value2.amount , 3) => x => x_10;",
                x => x.Functions.Values.First().Should().BeEquivalentTo(new object[]{"value1.amount + value2.amount", "3"}));
        }

        private static ConfigurationString Having(string input, params Action<ConfigurationString>[] actions)
        {
            var config = ConfigurationString.Parse(input);
            
            foreach (var action in actions)
                action(config);

            return config;
        }
    }
}