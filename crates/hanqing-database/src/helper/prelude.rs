//! Helper traits, one per entity; import with `use helper::prelude::*` and
//! call on the entity (`Messages::save_all(db, batch)`, ...).

pub use super::media::MediaHelper;
pub use super::message::MessagesHelper;
pub use super::session::{
    SessionDcHomeHelper, SessionDcOptionHelper, SessionPeerHelper, SessionUpdateStateHelper,
};
