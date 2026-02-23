require "minitest/autorun"
require "pg"
require "tmpdir"
require "fileutils"
require "open3"

class GitgresTest < Minitest::Test
  def setup
    @conn = PG.connect(dbname: "gitgres_test")
    @conn.exec("BEGIN")
    # Create a test repository
    result = @conn.exec("INSERT INTO repositories (name) VALUES ('test_repo') RETURNING id")
    @repo_id = result[0]["id"].to_i
  end

  def teardown
    @conn.exec("ROLLBACK")
    @conn.close
  end

  # Helper to write a git object and return its OID as hex
  def write_object(type, content)
    result = @conn.exec_params(
      "SELECT encode(git_object_write($1, $2::smallint, $3::bytea), 'hex') AS oid",
      [@repo_id, type, { value: content, format: 1 }]  # format: 1 for binary
    )
    result[0]["oid"]
  end

  # Helper to get the expected git hash for content
  def git_hash_object(type, content)
    type_name = { 1 => "commit", 2 => "tree", 3 => "blob", 4 => "tag" }[type]
    out, status = Open3.capture2("git", "hash-object", "--stdin", "-t", type_name, stdin_data: content)
    out.strip
  end

  # Create a temporary git repo for testing
  def create_test_repo
    dir = Dir.mktmpdir("gitgres_test")
    system("git", "init", dir, out: File::NULL, err: File::NULL)
    system("git", "-C", dir, "config", "user.email", "test@test.com")
    system("git", "-C", dir, "config", "user.name", "Test User")
    dir
  end
end
