use std::task::Wake;

use apache_avro::{to_value, Schema, Writer};
use serde::{Deserialize, Serialize, Serializer};

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub enum MyDouble {
    Some(f64),
    None,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub enum MyFloat {
    Some(f32),
    None,
}

impl Serialize for MyDouble {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 3 is the number of fields in the struct.
        match self {
            MyDouble::Some(value) => serializer.serialize_f64(*value),
            MyDouble::None => serializer.serialize_unit(),
        }
    }
}

impl Serialize for MyFloat {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 3 is the number of fields in the struct.
        match self {
            MyFloat::Some(value) => serializer.serialize_f32(*value),
            MyFloat::None => serializer.serialize_unit(),
        }
    }
}

#[derive(Deserialize, Serialize)]
struct Topic {
    double0: Option<f64>,
    float0: Option<f32>,
}

#[derive(Deserialize, Serialize)]
struct TopicNonOptional {
    double0: f64,
    float0: f32,
}

#[derive(Deserialize, Serialize)]
struct TopicMyOption {
    double0: MyDouble,
    float0: MyFloat,
}

impl Default for MyDouble {
    fn default() -> Self {
        MyDouble::Some(0.0)
    }
}

impl Default for MyFloat {
    fn default() -> Self {
        MyFloat::Some(0.0)
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
        },
        {
            "name": "float0",
            "type": [
                "float",
                "null"
            ],
            "default": 0.0
        }
    ]
}"#;
    let schema = Schema::parse_str(&topic_schema).unwrap();

    let topic: Topic = Topic {
        double0: Some(1234.5),
        float0: Some(5.4321),
    };

    let topic_nop = TopicNonOptional {
        double0: 1234.5,
        float0: 5.4321,
    };

    let topic_myop = TopicMyOption {
        double0: MyDouble::Some(1234.5),
        float0: MyFloat::Some(5.4321),
    };

    let topic_myop_none = TopicMyOption {
        double0: MyDouble::None,
        float0: MyFloat::None,
    };
    let value = to_value(&topic);

    let value_nop = to_value(&topic_nop);

    let value_myup = to_value(&topic_myop);

    let value_myup_none = to_value(&topic_myop_none);

    println!("value={value:?}");
    println!("value no option={value_nop:?}");
    println!("value my optional={value_myup:?}");
    println!("value my optional none={value_myup_none:?}");

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
    assert!(writter_res.is_ok());

    let mut writer = Writer::new(&schema, Vec::new());
    // This will fail validation because the type in not compatible.
    let writter_res = writer.append_ser(topic_myop_none);

    println!("{writter_res:?}");
    assert!(writter_res.is_ok());
}
