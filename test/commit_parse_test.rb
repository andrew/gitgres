require_relative "test_helper"

class CommitParseTest < GitgresTest
  def test_parse_simple_commit
    dir = create_test_repo
    File.write(File.join(dir, "test.txt"), "test")
    system("git", "-C", dir, "add", ".", out: File::NULL, err: File::NULL)

    env = { "GIT_AUTHOR_DATE" => "1234567890 +0000", "GIT_COMMITTER_DATE" => "1234567890 +0000" }
    system(env, "git", "-C", dir, "commit", "-m", "initial commit", out: File::NULL, err: File::NULL)

    commit_oid = `git -C #{dir} rev-parse HEAD`.strip
    commit_content = `git -C #{dir} cat-file commit #{commit_oid}`
    tree_oid = `git -C #{dir} rev-parse HEAD^{tree}`.strip

    result = @conn.exec_params(
      "SELECT encode(tree_oid, 'hex') AS tree_oid, parent_oids, author_name, author_email, author_timestamp, committer_name, committer_email, message FROM git_commit_parse($1::bytea)",
      [{ value: commit_content, format: 1 }]
    )

    assert_equal 1, result.ntuples
    assert_equal tree_oid, result[0]["tree_oid"]
    assert_equal "Test User", result[0]["author_name"]
    assert_equal "test@test.com", result[0]["author_email"]
    assert_equal "1234567890", result[0]["author_timestamp"]
    assert_match(/initial commit/, result[0]["message"])

    FileUtils.rm_rf(dir)
  end

  def test_parse_commit_with_parent
    dir = create_test_repo
    File.write(File.join(dir, "test.txt"), "v1")
    system("git", "-C", dir, "add", ".", out: File::NULL, err: File::NULL)
    system("git", "-C", dir, "commit", "-m", "first", out: File::NULL, err: File::NULL)

    first_oid = `git -C #{dir} rev-parse HEAD`.strip

    File.write(File.join(dir, "test.txt"), "v2")
    system("git", "-C", dir, "add", ".", out: File::NULL, err: File::NULL)
    system("git", "-C", dir, "commit", "-m", "second", out: File::NULL, err: File::NULL)

    commit_oid = `git -C #{dir} rev-parse HEAD`.strip
    commit_content = `git -C #{dir} cat-file commit #{commit_oid}`

    result = @conn.exec_params(
      "SELECT parent_oids FROM git_commit_parse($1::bytea)",
      [{ value: commit_content, format: 1 }]
    )

    # parent_oids should contain the first commit's OID
    parent_oids_raw = result[0]["parent_oids"]
    assert_match(/#{first_oid}/, parent_oids_raw.gsub("\\\\x", ""))

    FileUtils.rm_rf(dir)
  end
end
