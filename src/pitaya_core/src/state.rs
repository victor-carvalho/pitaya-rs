use crate::context::{Context, FromContext, NotFound};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct State<'a, T: Send + Sync + 'static>(&'a T);

impl<'a, T: Send + Sync + 'static> State<'a, T> {}

impl<T: Send + Sync + 'static> std::ops::Deref for State<'_, T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &T {
        self.0
    }
}

impl<'a, T: Send + Sync + 'static> FromContext<'a> for State<'a, T> {
    fn from_context(ctx: &'a Context) -> Result<Self, NotFound> {
        match ctx.try_get_state::<T>() {
            Some(val) => Ok(State(val)),
            None => Err(NotFound(String::from("could not find managed state"))),
        }
    }
}
