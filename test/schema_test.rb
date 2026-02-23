require_relative "test_helper"

class SchemaTest < GitgresTest
  def test_repositories_table_exists
    result = @conn.exec("SELECT count(*) FROM repositories")
    assert result
  end

  def test_objects_table_exists
    result = @conn.exec("SELECT count(*) FROM objects WHERE repo_id = #{@repo_id}")
    assert_equal 0, result[0]["count"].to_i
  end

  def test_refs_table_exists
    result = @conn.exec("SELECT count(*) FROM refs WHERE repo_id = #{@repo_id}")
    assert_equal 0, result[0]["count"].to_i
  end

  def test_reflog_table_exists
    result = @conn.exec("SELECT count(*) FROM reflog WHERE repo_id = #{@repo_id}")
    assert_equal 0, result[0]["count"].to_i
  end

  def test_objects_rejects_invalid_type_ref
    # Objects table requires valid repo_id foreign key
    assert_raises(PG::ForeignKeyViolation) do
      @conn.exec("INSERT INTO objects (repo_id, oid, type, size, content) VALUES (99999, decode('aa', 'hex'), 1, 0, ''::bytea)")
    end
  end

  def test_refs_check_constraint
    # Must have exactly one of oid or symbolic
    assert_raises(PG::CheckViolation) do
      @conn.exec_params(
        "INSERT INTO refs (repo_id, name, oid, symbolic) VALUES ($1, 'refs/test', NULL, NULL)",
        [@repo_id]
      )
    end
  end

  def test_refs_check_constraint_both_set
    assert_raises(PG::CheckViolation) do
      @conn.exec_params(
        "INSERT INTO refs (repo_id, name, oid, symbolic) VALUES ($1, 'refs/test', decode('0000000000000000000000000000000000000000', 'hex'), 'refs/heads/main')",
        [@repo_id]
      )
    end
  end
end
