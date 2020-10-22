use async_trait::async_trait;
use std::{
    ffi::{c_void, CString},
    os::raw::c_char,
};

#[repr(C)]
pub struct PitayaMetricsOpts {
    pub namespace: *const c_char,
    pub subsystem: *const c_char,
    pub name: *const c_char,
    pub help: *const c_char,
    pub variable_labels: *mut *mut c_char,
    pub variable_labels_count: u32,
    pub buckets: *mut f64,
    pub buckets_count: u32,
}

type PitayaRegisterFn = extern "C" fn(user_data: *mut c_void, opts: PitayaMetricsOpts);

type PitayaIncCounterFn = extern "C" fn(
    user_date: *mut c_void,
    name: *const c_char,
    labels: *mut *const c_char,
    labels_count: u32,
);

type PitayaObserveHistFn = extern "C" fn(
    user_data: *mut c_void,
    name: *const c_char,
    value: f64,
    labels: *mut *const c_char,
    labels_count: u32,
);

type PitayaSetGaugeFn = extern "C" fn(
    user_data: *mut c_void,
    name: *const c_char,
    value: f64,
    labels: *mut *const c_char,
    labels_count: u32,
);

type PitayaAddGaugeFn = extern "C" fn(
    user_data: *mut c_void,
    name: *const c_char,
    value: f64,
    labels: *mut *const c_char,
    labels_count: u32,
);

pub struct PitayaMetricsReporter {
    register_counter_fn: PitayaRegisterFn,
    register_histogram_fn: PitayaRegisterFn,
    register_gauge_fn: PitayaRegisterFn,
    inc_counter_fn: PitayaIncCounterFn,
    observe_hist_fn: PitayaObserveHistFn,
    set_gauge_fn: PitayaSetGaugeFn,
    add_gauge_fn: PitayaAddGaugeFn,
    user_data: super::PitayaUserData,
}

fn generic_register(
    register_fn: extern "C" fn(*mut c_void, PitayaMetricsOpts),
    user_data: *mut c_void,
    mut opts: crate::metrics::Opts,
) {
    // TODO(lhahn): this function allocates a lot of unnecessary memory.
    // consider improving this in the future.

    let mut variable_labels_ptr: Vec<*mut c_char> = opts
        .variable_labels
        .iter_mut()
        .map(|s| s.as_mut_ptr() as *mut c_char)
        .collect();

    // These options will be passed to C#.
    // We cannot simply get a raw pointer to the &str, since C expects
    // strings to end with a 0 byte, which is not the case for Rust strings.
    let namespace =
        CString::new(opts.namespace.as_str()).expect("string should be valid inside rust");
    let subsystem =
        CString::new(opts.subsystem.as_str()).expect("string should be valid inside rust");
    let name = CString::new(opts.name.as_str()).expect("string should be valid inside rust");
    let help = CString::new(opts.help.as_str()).expect("string should be valid inside rust");

    let opts = PitayaMetricsOpts {
        namespace: namespace.as_ptr(),
        subsystem: subsystem.as_ptr(),
        name: name.as_ptr(),
        help: help.as_ptr(),
        variable_labels: variable_labels_ptr.as_mut_ptr(),
        variable_labels_count: variable_labels_ptr.len() as u32,
        buckets: opts.buckets.as_mut_ptr(),
        buckets_count: opts.buckets.len() as u32,
    };

    register_fn(user_data, opts);
}

#[async_trait]
impl crate::metrics::Reporter for PitayaMetricsReporter {
    fn register_counter(
        &mut self,
        opts: crate::metrics::Opts,
    ) -> Result<(), crate::metrics::Error> {
        generic_register(self.register_counter_fn, self.user_data.0, opts);
        Ok(())
    }

    fn register_histogram(
        &mut self,
        opts: crate::metrics::Opts,
    ) -> Result<(), crate::metrics::Error> {
        generic_register(self.register_histogram_fn, self.user_data.0, opts);
        Ok(())
    }

    fn register_gauge(&mut self, opts: crate::metrics::Opts) -> Result<(), crate::metrics::Error> {
        generic_register(self.register_gauge_fn, self.user_data.0, opts);
        Ok(())
    }

    async fn start(&mut self) -> Result<(), crate::metrics::Error> {
        // TODO(lhahn): consider implementing start for FFI.
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), crate::metrics::Error> {
        // TODO(lhahn): consider implementing shutdown for FFI.
        Ok(())
    }

    fn inc_counter(&self, name: &str, labels: &[&str]) -> Result<(), crate::metrics::Error> {
        let mut labels_ptr: Vec<*const c_char> =
            labels.iter().map(|s| s.as_ptr() as *const c_char).collect();
        let name = CString::new(name).expect("string in rust should be valid");

        (self.inc_counter_fn)(
            self.user_data.0,
            name.as_ptr() as *const c_char,
            labels_ptr.as_mut_ptr(),
            labels_ptr.len() as u32,
        );

        Ok(())
    }

    fn observe_hist(
        &self,
        name: &str,
        value: f64,
        labels: &[&str],
    ) -> Result<(), crate::metrics::Error> {
        let mut labels_ptr: Vec<*const c_char> =
            labels.iter().map(|s| s.as_ptr() as *const c_char).collect();
        let name = CString::new(name).expect("string in rust should be valid");

        (self.observe_hist_fn)(
            self.user_data.0,
            name.as_ptr() as *const c_char,
            value,
            labels_ptr.as_mut_ptr(),
            labels_ptr.len() as u32,
        );

        Ok(())
    }

    fn set_gauge(
        &self,
        name: &str,
        value: f64,
        labels: &[&str],
    ) -> Result<(), crate::metrics::Error> {
        let mut labels_ptr: Vec<*const c_char> =
            labels.iter().map(|s| s.as_ptr() as *const c_char).collect();
        let name = CString::new(name).expect("string in rust should be valid");

        (self.set_gauge_fn)(
            self.user_data.0,
            name.as_ptr() as *const c_char,
            value,
            labels_ptr.as_mut_ptr(),
            labels_ptr.len() as u32,
        );

        Ok(())
    }

    fn add_gauge(
        &self,
        name: &str,
        value: f64,
        labels: &[&str],
    ) -> Result<(), crate::metrics::Error> {
        let mut labels_ptr: Vec<*const c_char> =
            labels.iter().map(|s| s.as_ptr() as *const c_char).collect();
        let name = CString::new(name).expect("string in rust should be valid");

        (self.add_gauge_fn)(
            self.user_data.0,
            name.as_ptr() as *const c_char,
            value,
            labels_ptr.as_mut_ptr(),
            labels_ptr.len() as u32,
        );

        Ok(())
    }
}

#[no_mangle]
pub extern "C" fn pitaya_metrics_reporter_new(
    register_counter_fn: PitayaRegisterFn,
    register_histogram_fn: PitayaRegisterFn,
    register_gauge_fn: PitayaRegisterFn,
    inc_counter_fn: PitayaIncCounterFn,
    observe_hist_fn: PitayaObserveHistFn,
    set_gauge_fn: PitayaSetGaugeFn,
    add_gauge_fn: PitayaAddGaugeFn,
    user_data: *mut c_void,
) -> *mut PitayaMetricsReporter {
    Box::into_raw(Box::new(PitayaMetricsReporter {
        register_counter_fn,
        register_histogram_fn,
        register_gauge_fn,
        inc_counter_fn,
        observe_hist_fn,
        set_gauge_fn,
        add_gauge_fn,
        user_data: super::PitayaUserData(user_data),
    }))
}

#[no_mangle]
pub extern "C" fn pitaya_metrics_reporter_drop(ptr: *mut PitayaMetricsReporter) {
    let _ = unsafe { Box::from_raw(ptr) };
}
