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
    TransformStep,
    IntoFn,
    IntoMap,
    IntoPipeline,
    QUEUE_SIZE,
};

use std::{
    marker::PhantomData,
    thread::{spawn, JoinHandle},
};

use crossbeam_channel::{bounded, Sender, Receiver};

pub struct Transform<S, F, Xin, Xout, P>
    where
        P: TransformStep<S, Xin>,
        F: Fn(Xin) -> Xout,
{
    previous: P,
    transform: F,
    _a: PhantomData<S>,
    _b: PhantomData<Xin>,
    _c: PhantomData<Xout>,
}

impl<S, F, Xin, Xout, P> Transform<S, F, Xin, Xout, P>
    where
        P: TransformStep<S, Xin>,
        F: Fn(Xin) -> Xout,
{
    pub(super) fn new(p: P, f: F) -> Self {
        Transform {
            previous: p,
            transform: f,
            _a: PhantomData,
            _b: PhantomData,
            _c: PhantomData,
        }
    }
}

impl<S, F, Xin, Xout, P> TransformStep<S, Xout> for Transform<S, F, Xin, Xout, P>
    where
        P: TransformStep<S, Xin>,
        F: Fn(Xin) -> Xout,
{}

impl<S, F, Xin, Xout, P> IntoFn<S, Xout> for Transform<S, F, Xin, Xout, P>
    where
        P: TransformStep<S, Xin> + IntoFn<S, Xin>,
        F: Fn(Xin) -> Xout + 'static + Send + Sync,
        S: 'static,
        Xin: 'static,
{
    fn into_fn(self) -> Box<Fn(S) -> Xout + Send + Sync> {
        let inner = self.previous.into_fn();
        let tf = self.transform;
        Box::new(move |s| tf(inner(s)))
    }
}

impl<S, F, Xin, Xout, P> IntoMap<S, Xout> for Transform<S, F, Xin, Xout, P>
    where
        P: TransformStep<S, Xin> + IntoMap<S, Xin>,
        F: Fn(Xin) -> Xout + 'static,
        Xin: 'static,
{
    fn into_map<I: IntoIterator<Item=S> + 'static>(self, src: I) -> Box<Iterator<Item=Xout>> {
        let inner = self.previous.into_map(src);
        let tf = self.transform;
        Box::new(inner.map(tf))
    }
}

impl<S, F, Xin, Xout, P> IntoPipeline<S, Xout> for Transform<S, F, Xin, Xout, P>
    where
        P: TransformStep<S, Xin> + IntoPipeline<S, Xin>,
        F: Fn(Xin) -> Xout + Send + 'static,
        Xin: Send + 'static,
        Xout: Send + 'static,
{
    fn into_pipeline(self) -> (Sender<S>, Receiver<Xout>, Vec<JoinHandle<()>>) {
        let (send, thr_recv, mut thr_hdl) = self.previous.into_pipeline();
        let (thr_send, recv) = bounded(QUEUE_SIZE);
        let tf = self.transform;
        thr_hdl.push(spawn(move || {
            for val in thr_recv {
                if thr_send.send(tf(val)).is_err() {
                    break;
                }
            }
        }));
        (send, recv, thr_hdl)
    }
}
