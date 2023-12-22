use apache_avro::{to_value, Schema, Writer};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub enum MyOption<T> {
    Some(T),
    None,
}

#[derive(Deserialize, Serialize)]
struct Topic {
    double0: Option<f64>,
}

#[derive(Deserialize, Serialize)]
struct TopicNonOptional {
    double0: f64,
}

#[derive(Deserialize, Serialize)]
struct TopicMyOption {
    double0: MyOption<f64>,
}

impl<T: Default> Default for MyOption<T> {
    fn default() -> Self {
        MyOption::Some(T::default())
    }
}

fn main() {
    let topic_schema = r#"{
    "type": "record",
    "name": "Topic",
    "namespace": "some.namespace",
    "fields": [
        {
            "name": "double0",
            "type": [
                "double",
                "null"
            ],
            "default": 0.0
        }
    ]
}"#;
    let schema = Schema::parse_str(&topic_schema).unwrap();

    let topic: Topic = Topic {
        double0: Some(1234.5),
    };

    let topic_nop = TopicNonOptional { double0: 1234.5 };

    let topic_myop = TopicMyOption {
        double0: MyOption::Some(1234.5),
    };

    let value = to_value(&topic);

    let value_nop = to_value(&topic_nop);

    let value_myup = to_value(&topic_myop);

    println!("{value:?}");
    println!("{value_nop:?}");
    println!("{value_myup:?}");

    let mut writer = Writer::new(&schema, Vec::new());
    // This works but TopicNonOptional does not support missing double0.
    writer.append_ser(topic_nop).unwrap();

    let encoded = writer.into_inner();

    println!("{encoded:?}");

    let mut writer = Writer::new(&schema, Vec::new());
    // This will fail validation because the type in not compatible.
    let writter_res = writer.append_ser(topic);

    println!("{writter_res:?}");
    assert!(writter_res.is_err());

    let mut writer = Writer::new(&schema, Vec::new());
    // This will fail validation because the type in not compatible.
    let writter_res = writer.append_ser(topic_myop);

    println!("{writter_res:?}");
    assert!(writter_res.is_err());
}
