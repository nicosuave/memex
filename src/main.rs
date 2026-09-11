fn main() -> anyhow::Result<()> {
    #[cfg(feature = "profiling")]
    let profile = memex::profiling::Session::from_env()?;
    let result = memex::cli::run();
    #[cfg(feature = "profiling")]
    {
        let trace_result = profile.finish();
        result?;
        trace_result
    }
    #[cfg(not(feature = "profiling"))]
    result
}
