export function parse(_sql: string) {
  return { statements: [], error: null };
}

export function inject_progress(sql: string): string {
  return sql;
}
