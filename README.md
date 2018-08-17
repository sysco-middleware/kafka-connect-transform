# kafka-connect-transform

[![Build Status](https://travis-ci.org/sysco-middleware/kafka-connect-transform.svg?branch=master)](https://travis-ci.org/sysco-middleware/kafka-connect-transform)

## Single message transforms for Kafka Connect

This is a collection of Single Message Transforms for Kafka Connect. So far it contains:
* RegexTransform

### RegexTransform

This transformer will match the regex on the string field and perform the replacement with the text in field.replacement. It can either replace all occurrences of the regex or only the first one.

Name | Description | Type | Valid values | Importance
---- | ----------- | ---- | ------------ | ------------
field | The field containing the string to apply replacement, or empty if the entire value is a string. | String |  | High
field.regex | Regular expression to use for matching. | String | | High
field.replacement | Replacement string. | String | | High
field.replacement.method | Replacement method tells whether to replace all the occurrences of the matched string or only the first one. Default value: ALL. | String | ALL, FIRST | Medium

#### Examples

Let's say you have a field FIELD1 that can have value: 123abc123 and you want to replace only the first occurrance of 123 string with 789 to get 789abc123. In that case your connector config would look like (showing only transforms part):
```
"name": "my-connector-with-regex",
  "config": {
	"transforms": "replace_first_number",		
	"transforms.replace_first_number.type": "no.sysco.transform.RegexTransform$Value",
	"transforms.replace_first_number.field": "FIELD1",
	"transforms.replace_first_number.field.regex": "123", 
	"transforms.replace_first_number.field.replacement": "789",
	"transforms.replace_first_number.field.replacement.method": "FIRST"
  }
} 
```
