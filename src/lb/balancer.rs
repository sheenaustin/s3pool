use super::backend::Backend;
use rand::Rng;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Load balancing algorithm selection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Algorithm {
    /// Simple round-robin distribution
    RoundRobin,
    /// Pick 2 random backends, choose the one with fewer connections
    PowerOfTwo,
    /// Always pick the backend with the least connections
    LeastConnections,
}

/// Load balancer for distributing requests across backends
pub struct LoadBalancer {
    /// List of backends to balance across
    backends: Arc<Vec<Backend>>,
    /// Selected load balancing algorithm
    algorithm: Algorithm,
    /// Counter for round-robin algorithm
    counter: AtomicUsize,
}

impl LoadBalancer {
    /// Create a new load balancer with the given backends and algorithm
    pub fn new(backends: Vec<Backend>, algorithm: Algorithm) -> Self {
        Self {
            backends: Arc::new(backends),
            algorithm,
            counter: AtomicUsize::new(0),
        }
    }

    /// Select a backend using the configured algorithm
    /// Returns the index of the selected backend
    pub fn select_backend(&self) -> Option<usize> {
        if self.backends.is_empty() {
            return None;
        }

        match self.algorithm {
            Algorithm::RoundRobin => self.select_round_robin(),
            Algorithm::PowerOfTwo => self.select_power_of_two(),
            Algorithm::LeastConnections => self.select_least_connections(),
        }
    }

    /// Select a backend and filter by health
    /// Returns the index of the selected healthy backend
    pub fn select_healthy_backend(&self) -> Option<usize> {
        if self.backends.is_empty() {
            return None;
        }

        // Try to find a healthy backend using the algorithm
        match self.algorithm {
            Algorithm::RoundRobin => self.select_healthy_round_robin(),
            Algorithm::PowerOfTwo => self.select_healthy_power_of_two(),
            Algorithm::LeastConnections => self.select_healthy_least_connections(),
        }
    }

    /// Get a reference to a backend by index
    pub fn get_backend(&self, index: usize) -> Option<&Backend> {
        self.backends.get(index)
    }

    /// Get all backends
    pub fn get_backends(&self) -> &[Backend] {
        &self.backends
    }

    /// Get the number of backends
    pub fn backend_count(&self) -> usize {
        self.backends.len()
    }

    /// Round-robin: simple counter-based selection
    fn select_round_robin(&self) -> Option<usize> {
        let index = self.counter.fetch_add(1, Ordering::Relaxed) % self.backends.len();
        Some(index)
    }

    /// Round-robin with health check: try up to N backends
    fn select_healthy_round_robin(&self) -> Option<usize> {
        let len = self.backends.len();
        let start_index = self.counter.fetch_add(1, Ordering::Relaxed) % len;

        // Try each backend starting from the round-robin position
        for i in 0..len {
            let index = (start_index + i) % len;
            if self.backends[index].is_healthy() {
                return Some(index);
            }
        }

        // No healthy backends found, return least unhealthy
        self.select_least_unhealthy()
    }

    /// Power-of-two: pick 2 random, choose the one with fewer connections
    fn select_power_of_two(&self) -> Option<usize> {
        let len = self.backends.len();
        if len == 1 {
            return Some(0);
        }

        let mut rng = rand::thread_rng();
        let a = rng.gen_range(0..len);
        let b = rng.gen_range(0..len);

        // Choose the backend with fewer active connections
        if self.backends[a].get_active_connections()
            <= self.backends[b].get_active_connections()
        {
            Some(a)
        } else {
            Some(b)
        }
    }

    /// Power-of-two with health check
    fn select_healthy_power_of_two(&self) -> Option<usize> {
        let len = self.backends.len();
        if len == 1 {
            return if self.backends[0].is_healthy() {
                Some(0)
            } else {
                None
            };
        }

        let mut rng = rand::thread_rng();

        // Try up to 5 times to find two healthy backends
        for _ in 0..5 {
            let a = rng.gen_range(0..len);
            let b = rng.gen_range(0..len);

            let a_healthy = self.backends[a].is_healthy();
            let b_healthy = self.backends[b].is_healthy();

            match (a_healthy, b_healthy) {
                (true, true) => {
                    // Both healthy, choose the one with fewer connections
                    return if self.backends[a].get_active_connections()
                        <= self.backends[b].get_active_connections()
                    {
                        Some(a)
                    } else {
                        Some(b)
                    };
                }
                (true, false) => return Some(a),
                (false, true) => return Some(b),
                (false, false) => continue,
            }
        }

        // Fallback to any healthy backend
        self.backends
            .iter()
            .position(|b| b.is_healthy())
            .or_else(|| self.select_least_unhealthy())
    }

    /// Least-connections: pick the backend with the fewest active connections
    fn select_least_connections(&self) -> Option<usize> {
        self.backends
            .iter()
            .enumerate()
            .min_by_key(|(_, b)| b.get_active_connections())
            .map(|(idx, _)| idx)
    }

    /// Least-connections with health check
    fn select_healthy_least_connections(&self) -> Option<usize> {
        self.backends
            .iter()
            .enumerate()
            .filter(|(_, b)| b.is_healthy())
            .min_by_key(|(_, b)| b.get_active_connections())
            .map(|(idx, _)| idx)
            .or_else(|| self.select_least_unhealthy())
    }

    /// Fallback: select the backend with the highest health score
    fn select_least_unhealthy(&self) -> Option<usize> {
        self.backends
            .iter()
            .enumerate()
            .max_by_key(|(_, b)| b.get_health_score())
            .map(|(idx, _)| idx)
    }
}

impl Clone for LoadBalancer {
    fn clone(&self) -> Self {
        Self {
            backends: Arc::clone(&self.backends),
            algorithm: self.algorithm,
            counter: AtomicUsize::new(self.counter.load(Ordering::Relaxed)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_backends(count: usize) -> Vec<Backend> {
        (0..count)
            .map(|i| Backend::new(format!("http://s3-{}:9000", i + 1)))
            .collect()
    }

    #[test]
    fn test_round_robin() {
        let backends = create_test_backends(3);
        let lb = LoadBalancer::new(backends, Algorithm::RoundRobin);

        // Should cycle through backends in order
        assert_eq!(lb.select_backend(), Some(0));
        assert_eq!(lb.select_backend(), Some(1));
        assert_eq!(lb.select_backend(), Some(2));
        assert_eq!(lb.select_backend(), Some(0));
        assert_eq!(lb.select_backend(), Some(1));
    }

    #[test]
    fn test_least_connections() {
        let backends = create_test_backends(3);
        let lb = LoadBalancer::new(backends, Algorithm::LeastConnections);

        // All backends have 0 connections, should return first
        let idx = lb.select_backend().unwrap();
        let backend = lb.get_backend(idx).unwrap();

        // Simulate connections
        backend.increment_connections();
        backend.increment_connections();

        // Next selection should prefer a different backend
        let next_idx = lb.select_backend().unwrap();
        assert_ne!(idx, next_idx);
    }

    #[test]
    fn test_power_of_two() {
        let backends = create_test_backends(10);
        let lb = LoadBalancer::new(backends, Algorithm::PowerOfTwo);

        // Should always return a valid index
        for _ in 0..100 {
            let idx = lb.select_backend().unwrap();
            assert!(idx < 10);
        }
    }

    #[test]
    fn test_healthy_selection() {
        let backends = create_test_backends(3);
        let lb = LoadBalancer::new(backends, Algorithm::RoundRobin);

        // Mark backend 1 as unhealthy
        lb.get_backend(1).unwrap().set_health_score(0);

        // Should skip unhealthy backend
        let mut selected = vec![];
        for _ in 0..10 {
            if let Some(idx) = lb.select_healthy_backend() {
                selected.push(idx);
            }
        }

        // Should never select backend 1
        assert!(!selected.contains(&1));
        assert!(selected.contains(&0) || selected.contains(&2));
    }

    #[test]
    fn test_all_unhealthy() {
        let backends = create_test_backends(3);
        let lb = LoadBalancer::new(backends, Algorithm::RoundRobin);

        // Mark all backends as unhealthy with different scores
        lb.get_backend(0).unwrap().set_health_score(10);
        lb.get_backend(1).unwrap().set_health_score(20);
        lb.get_backend(2).unwrap().set_health_score(5);

        // Should return the least unhealthy (backend 1 with score 20)
        let idx = lb.select_healthy_backend().unwrap();
        assert_eq!(idx, 1);
    }

    #[test]
    fn test_empty_backends() {
        let backends = vec![];
        let lb = LoadBalancer::new(backends, Algorithm::RoundRobin);

        assert_eq!(lb.select_backend(), None);
        assert_eq!(lb.select_healthy_backend(), None);
    }

    #[test]
    fn test_single_backend() {
        let backends = create_test_backends(1);
        let lb = LoadBalancer::new(backends, Algorithm::RoundRobin);

        assert_eq!(lb.select_backend(), Some(0));
        assert_eq!(lb.select_backend(), Some(0));
        assert_eq!(lb.select_backend(), Some(0));
    }
}
