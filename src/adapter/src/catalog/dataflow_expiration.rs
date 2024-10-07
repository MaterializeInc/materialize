use crate::catalog::Catalog;
use mz_repr::GlobalId;

impl Catalog {
    /// Whether the catalog entry `id` or any of its transitive dependencies is a materialized view
    /// with a refresh schedule. Used to disable dataflow expiration if found.
    pub(crate) fn item_has_transitive_refresh_schedule(&self, id: GlobalId) -> bool {
        let test_has_transitive_refresh_schedule = |dep: GlobalId| -> bool {
            if let Some(mv) = self.get_entry(&dep).materialized_view() {
                return mv.refresh_schedule.is_some();
            }
            false
        };
        test_has_transitive_refresh_schedule(id)
            || self
                .state()
                .transitive_uses(id)
                .any(test_has_transitive_refresh_schedule)
    }
}
