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

use crate::{start::Start, transform::Transform, IntoMap, IntoPipeline, TransformStep, QUEUE_SIZE};

use std::{
    marker::PhantomData,
    thread::{spawn, JoinHandle},
};

use crossbeam_channel::{bounded, Receiver, Sender};

pub struct Block<S, X, Y, P, I>
where
    P: TransformStep<S, X>,
    I: TransformStep<X, Y>,
{
    prev: P,
    inner: I,
    _s: PhantomData<S>,
    _x: PhantomData<X>,
    _y: PhantomData<Y>,
}

pub struct Builder<S, X, Y, P, I>
where
    P: TransformStep<S, X>,
    I: TransformStep<X, Y>,
{
    prev: P,
    inner: I,
    _s: PhantomData<S>,
    _x: PhantomData<X>,
    _y: PhantomData<Y>,
}

impl<S, X, P> Block<S, X, X, P, Start>
where
    P: TransformStep<S, X>,
{
    pub(super) fn build(p: P) -> Builder<S, X, X, P, Start> {
        Builder {
            prev: p,
            inner: Start,
            _s: PhantomData,
            _x: PhantomData,
            _y: PhantomData,
        }
    }
}

impl<S, X, Y, P, I> Builder<S, X, Y, P, I>
where
    P: TransformStep<S, X>,
    I: TransformStep<X, Y> + 'static,
    X: 'static,
    Y: 'static + Send,
{
    pub fn step<Z, F>(self, f: F) -> Builder<S, X, Z, P, Transform<X, F, Y, Z, I>>
    where
        F: Fn(Y) -> Z + Send + 'static,
        Z: 'static + Send,
    {
        Builder {
            prev: self.prev,
            inner: self.inner.step(f),
            _s: PhantomData,
            _x: PhantomData,
            _y: PhantomData,
        }
    }

    pub fn finish(self) -> Block<S, X, Y, P, I> {
        Block {
            prev: self.prev,
            inner: self.inner,
            _s: PhantomData,
            _x: PhantomData,
            _y: PhantomData,
        }
    }
}

impl<S, X, Y, P, I> TransformStep<S, Y> for Block<S, X, Y, P, I>
where
    P: TransformStep<S, X>,
    I: TransformStep<X, Y> + 'static + Send,
    X: 'static + Send,
    Y: 'static + Send,
{
}

impl<S, X, Y, P, I> IntoPipeline<S, Y> for Block<S, X, Y, P, I>
where
    P: TransformStep<S, X> + IntoPipeline<S, X>,
    I: TransformStep<X, Y> + IntoMap<X, Y> + 'static + Send,
    X: 'static + Send,
    Y: 'static + Send,
{
    fn into_pipeline(self) -> (Sender<S>, Receiver<Y>, Vec<JoinHandle<()>>) {
        let (send, thr_recv, mut thr_hdl) = self.prev.into_pipeline();
        let (thr_send, recv) = bounded(QUEUE_SIZE);
        let inner = self.inner;
        thr_hdl.push(spawn(move || {
            for val in inner.into_map(thr_recv) {
                if thr_send.send(val).is_err() {
                    break;
                }
            }
        }));
        (send, recv, thr_hdl)
    }
}
