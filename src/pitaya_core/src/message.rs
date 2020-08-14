// A MessageKind can be either a Request that receives a response
// or a Notify, in which the server calls an RPC without expecting response.
#[derive(Debug, PartialEq)]
pub enum Kind {
    Request = 0,
    Notify = 1,
}

// Represents a message that is going to be sent to another server.
// This can be either a message received from a client (device), or another server as well.
pub struct Message {
    pub kind: Kind,
    // Unique message id. Zero when notify.
    pub id: u32,
    // The route for finding the service alongside its handlers/remotes.
    pub route: String,
    // The data to be sent to another server. Typically a JSON or a Protobuf.
    pub data: Vec<u8>,
    // Is the message compressed?
    pub compressed: bool,
    // Is an error message?
    pub err: bool,
}

impl Default for Message {
    fn default() -> Self {
        Self {
            kind: Kind::Request,
            id: 0,
            route: String::new(),
            data: vec![],
            compressed: false,
            err: false,
        }
    }
}
