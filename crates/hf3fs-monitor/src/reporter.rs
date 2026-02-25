use crate::sample::Sample;

pub trait Reporter: Send + Sync {
    fn report(&self, samples: &[Sample]);
}

pub struct LogReporter;

impl Reporter for LogReporter {
    fn report(&self, samples: &[Sample]) {
        for sample in samples {
            tracing::info!(
                name = %sample.name,
                value = sample.value,
                "metric"
            );
        }
    }
}
