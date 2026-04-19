use crate::{Connection, Db, Frame, Parse};

use bytes::Bytes;
use tracing::debug;

#[derive(Debug)]
pub struct Del {
    keys: Vec<String>,
}

impl Del {
    pub fn new<K, I>(keys: I) -> Del
    where
        I: IntoIterator<Item = K>,
        K: Into<String>,
    {
        Del { keys }
    }

    // parse Del command
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Del> {
        let mut keys = vec![];
        loop {
            match parse.next_string() {
                Ok(key) => keys.push(key),
                Err(_) => break,
            }
        }

        Ok(Del { keys })
    }

    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let response = Frame::Integer(db.del(self.keys) as u64);

        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("del".as_bytes()));
        for key in self.keys {
            frame.push_bulk(Bytes::from(key));
        }
        frame
    }
}
