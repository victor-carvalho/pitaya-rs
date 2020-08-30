use crate::{context, protos, Route};
use std::{collections::HashMap, future::Future, pin::Pin};

pub type Method = fn(
    context::Context,
    &protos::Request,
) -> Pin<Box<dyn Future<Output = protos::Response> + Send + 'static>>;

pub struct StaticHandlerInfo {
    pub handler_name: &'static str,
    pub method_name: &'static str,
    pub method: Method,
}

pub struct Handlers(HashMap<String, HashMap<String, Method>>);

impl Handlers {
    pub fn new() -> Self {
        Handlers(HashMap::new())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn get(&self, route: &Route) -> Option<Method> {
        let handler = self.0.get(route.handler())?;
        handler.get(route.method()).map(|m| *m)
    }

    pub fn add(&mut self, info: &StaticHandlerInfo) {
        let handler_name = info.handler_name.to_string();
        assert!(!handler_name.is_empty(), "handler name should not be empty");

        self.0
            .entry(info.handler_name.to_string())
            .and_modify(|handler_hash| {
                let method_name = info.method_name.to_string();
                assert!(!method_name.is_empty(), "method name should not be empty");
                handler_hash
                    .entry(method_name.to_string())
                    .or_insert(info.method);
            })
            .or_insert_with(|| {
                let method_name = info.method_name.to_string();
                assert!(!method_name.is_empty(), "method name should not be empty");
                let mut handler = HashMap::new();
                handler.insert(method_name, info.method);
                handler
            });
    }
}
