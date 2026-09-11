#[cfg(feature = "profiling")]
mod capture;
#[cfg(feature = "profiling")]
pub use capture::Session;
#[cfg(feature = "profiling")]
pub(crate) use capture::{Scope, record_count};

#[cfg(feature = "profiling")]
macro_rules! span {
    ($name:literal) => {
        let _profile_scope = $crate::profiling::Scope::enter($name);
    };
}
#[cfg(not(feature = "profiling"))]
macro_rules! span {
    ($name:literal) => {{}};
}

#[cfg(feature = "profiling")]
macro_rules! count {
    ($name:literal, $value:expr) => {
        $crate::profiling::record_count($name, $value as u64)
    };
}
#[cfg(not(feature = "profiling"))]
macro_rules! count {
    ($name:literal, $value:expr) => {{}};
}

pub(crate) use {count, span};

#[cfg(all(test, not(feature = "profiling")))]
mod tests {
    #[test]
    fn disabled_arguments_are_not_evaluated_or_type_checked() {
        super::count!("unused", nonexistent_function());
        super::count!("unused", panic!("disabled instrumentation ran"));
        super::span!("unused");
    }
}
