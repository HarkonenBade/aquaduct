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

use crate::{
    start::Start,
    transform::Transform,
    TransformStep,
    IntoFn,
    IntoPipeline,
    QUEUE_SIZE,
};

use std::{
    marker::PhantomData,
    thread::{spawn, JoinHandle},
};

use crossbeam_channel::{bounded, Sender, Receiver};
use itertools::Itertools;
use rayon::prelude::*;

pub struct Block<S, X, Y, P, I>
    where
        P: TransformStep<S, X>,
        I: TransformStep<X, Y>,
{
    prev: P,
    inner: I,
    chunk_size: usize,
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
    chunk_size: usize,
    _s: PhantomData<S>,
    _x: PhantomData<X>,
    _y: PhantomData<Y>,
}

impl<S, X, P> Block<S, X, X, P, Start>
    where
        P: TransformStep<S, X>,
{
    pub(super) fn build(p: P, chunk_size: usize) -> Builder<S, X, X, P, Start> {
        Builder {
            prev: p,
            inner: Start,
            chunk_size,
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
            chunk_size: self.chunk_size,
            _s: PhantomData,
            _x: PhantomData,
            _y: PhantomData,
        }
    }

    pub fn finish(self) -> Block<S, X, Y, P, I> {
        Block {
            prev: self.prev,
            inner: self.inner,
            chunk_size: self.chunk_size,
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
{}

impl<S, X, Y, P, I> IntoPipeline<S, Y> for Block<S, X, Y, P, I>
    where
        P: TransformStep<S, X> + IntoPipeline<S, X>,
        I: TransformStep<X, Y> + IntoFn<X, Y> + 'static + Send,
        X: 'static + Send,
        Y: 'static + Send,
{
    fn into_pipeline(self) -> (Sender<S>, Receiver<Y>, Vec<JoinHandle<()>>) {
        let (send, pack_r, mut thr_hdl) = self.prev.into_pipeline();
        let (pack_w, tf_r) = bounded(QUEUE_SIZE);
        let (tf_w, unpack_r) = bounded(QUEUE_SIZE);
        let (unpack_w, recv) = bounded(QUEUE_SIZE);
        let tf = self.inner.into_fn();
        let chunk_size = self.chunk_size;
        thr_hdl.push(spawn(move || {
            for val in pack_r
                .into_iter()
                .batching(move |i| {
                    let v = i.take(chunk_size).collect::<Vec<_>>();
                    if v.is_empty() {
                        None
                    } else {
                        Some(v)
                    }
                }) {
                if pack_w.send(val).is_err() {
                    break;
                }
            }
        }));

        thr_hdl.push(spawn(move || {
            for val in tf_r {
                if tf_w.send(val.into_par_iter().map(|v| tf(v)).collect::<Vec<_>>()).is_err() {
                    break;
                }
            }
        }));

        thr_hdl.push(spawn(move || {
            for val in unpack_r.into_iter().flat_map(|v| v.into_iter()) {
                if unpack_w.send(val).is_err() {
                    break;
                }
            }
        }));
        (send, recv, thr_hdl)
    }
}
