//
// Copyright (c) 2018 Thomas Bytheway
// All rights reserved.
//
// This software was developed by BAE Systems, the University of Cambridge
// Computer Laboratory, and Memorial University under DARPA/AFRL contract
// FA8650-15-C-7558 ("CADETS"), as part of the DARPA Transparent Computing
// (TC) research program.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
// 1. Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
// OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
// HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
// LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
// OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//

use std::thread::{spawn, JoinHandle};

use crossbeam_channel::{bounded, Receiver, Sender};

const QUEUE_SIZE: usize = 1;

mod transform;
mod block;
mod par_block;

pub mod prelude {
    pub use crate::TransformStep as _aquaduct_TransformStep;
}

mod start {
    use super::*;

    pub struct Start;

    impl<S> TransformStep<S, S> for Start {}

    impl<S> IntoFn<S, S> for Start {
        fn into_fn(self) -> Box<Fn(S) -> S + Send + Sync> {
            Box::new(|s| s)
        }
    }

    impl<S> IntoMap<S, S> for Start
    {
        fn into_map<I: IntoIterator<Item=S> + 'static>(self, src: I) -> Box<Iterator<Item=S>> {
            Box::new(src.into_iter())
        }
    }

    impl<S> IntoPipeline<S, S> for Start {
        fn into_pipeline(self) -> (Sender<S>, Receiver<S>, Vec<JoinHandle<()>>) {
            let (send, recv) = bounded(QUEUE_SIZE);
            (send, recv, Vec::new())
        }
    }
}

pub trait TransformStep<S, X> {
    fn step<NX, G>(self, f: G) -> transform::Transform<S, G, X, NX, Self>
        where
            Self: Sized,
            G: Fn(X) -> NX,
    {
        transform::Transform::new(self, f)
    }

    fn block(self) -> block::Builder<S, X, X, Self, start::Start>
        where
            Self: Sized
    {
        block::Block::build(self)
    }

    fn par_block(self, chunk_size: usize) -> par_block::Builder<S, X, X, Self, start::Start>
        where
            Self: Sized
    {
        par_block::Block::build(self, chunk_size)
    }
}

pub trait IntoFn<S, X>: TransformStep<S, X> {
    fn into_fn(self) -> Box<Fn(S) -> X + Send + Sync>;
}

pub trait IntoMap<S, X>: TransformStep<S, X>
{
    fn into_map<I: IntoIterator<Item=S> + 'static>(self, src: I) -> Box<Iterator<Item = X>>;
}

pub trait IntoPipeline<S, X>: TransformStep<S, X> {
    fn into_pipeline(self) -> (Sender<S>, Receiver<X>, Vec<JoinHandle<()>>);
}

pub fn new_transform() -> start::Start {
    start::Start
}

pub fn pipeline<SS, S, DD, D, T>(mut src: S, tf: T, mut dst: D)
    where
        S: FnMut() -> Option<SS> + Send + 'static,
        D: FnMut(DD) + Send + 'static,
        T: IntoPipeline<SS, DD>,
        SS: Send + 'static,
        DD: Send + 'static,
{
    let (ch_s, ch_d, mut thr) = tf.into_pipeline();

    thr.push(spawn(move || {
        while let Some(v) = src() {
            if ch_s.send(v).is_err() {
                break;
            }
        }
    }));

    thr.push(spawn(move || {
        for v in ch_d {
            dst(v);
        }
    }));

    for t in thr {
        t.join().unwrap();
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn get_t() -> impl IntoFn<i64, String> + IntoMap<i64, String> + IntoPipeline<i64, String> {
        fn tf_mid(x: i64) -> i64 {
            x * 12
        }

        new_transform()
            .step(|x: i64| x + 10)
            .step(tf_mid)
            .step(|x| x.to_string())
    }

    #[test]
    fn test_fnmut() {
        let t = get_t();
        let f = t.into_fn();
        assert_eq!(&f(1), "132");
        assert_eq!(&f(2), "144");
    }

    #[test]
    fn test_map() {
        let t = get_t();
        let v = t.into_map(vec![1, 2, 3].into_iter()).collect::<Vec<_>>();
        assert_eq!(v[0], "132");
        assert_eq!(v[1], "144");
        assert_eq!(v[2], "156");
    }

    #[test]
    fn test_pipeline() {
        let r = get_t();
        let (vin, vout, threads) = r.into_pipeline();

        vin.send(1).unwrap();
        assert_eq!(vout.recv().unwrap(), "132");

        vin.send(2).unwrap();
        assert_eq!(vout.recv().unwrap(), "144");

        vin.send(3).unwrap();
        assert_eq!(vout.recv().unwrap(), "156");

        drop(vin);

        for t in threads {
            t.join().unwrap();
        }
    }

    #[test]
    fn test_block() {
        let t = new_transform()
            .step(|x: u32| x * 2)
            .block()
            .step(|x| x + 10)
            .step(|x| x * 5)
            .finish();

        let (vin, vout, thr) = t.into_pipeline();

        spawn(move || {
            for i in 0..3 {
                vin.send(i).unwrap();
            }
        });

        assert_eq!(vout.recv().unwrap(), 50);
        assert_eq!(vout.recv().unwrap(), 60);
        assert_eq!(vout.recv().unwrap(), 70);

        for t in thr {
            t.join().unwrap();
        }
    }
}
