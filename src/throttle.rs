//! Helpers to implement throttling within the Kafka protocol.

use std::time::Duration;

use tracing::warn;

use crate::backoff::ErrorOrThrottle;

pub fn maybe_throttle<E>(throttle_time_ms: Option<i32>) -> Result<(), ErrorOrThrottle<E>>
where
    E: Send,
{
    let throttle_time_ms = throttle_time_ms.unwrap_or_default();
    let throttle_time_ms: u64 = match throttle_time_ms.try_into() {
        Ok(t) => t,
        Err(_) => {
            warn!(throttle_time_ms, "Invalid throttle time",);
            return Ok(());
        }
    };

    if throttle_time_ms == 0 {
        return Ok(());
    }

    let duration = Duration::from_millis(throttle_time_ms);
    Err(ErrorOrThrottle::Throttle(duration))
}
