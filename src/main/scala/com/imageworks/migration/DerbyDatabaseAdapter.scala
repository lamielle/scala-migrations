/*
 * Copyright (c) 2010 Sony Pictures Imageworks Inc.
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the
 * distribution.  Neither the name of Sony Pictures Imageworks nor the
 * names of its contributors may be used to endorse or promote
 * products derived from this software without specific prior written
 * permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE
 * COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.imageworks.migration

class DerbyTimestampColumnDefinition
  extends ColumnDefinition
  with ColumnSupportsDefault
{
  // Derby does not take a size specifier for TIMESTAMP types.
  override
  def sql = sqlForColumnType("TIMESTAMP")
}

class DerbyVarbinaryColumnDefinition
  extends ColumnDefinition
  with ColumnSupportsDefault
  with ColumnSupportsLimit
{
  override
  def sql = sqlForColumnType("VARCHAR") + " FOR BIT DATA"
}

class DerbyDatabaseAdapter(override val schemaNameOpt: Option[String])
  extends DatabaseAdapter(schemaNameOpt)
{
  override
  val unquotedNameConverter = UppercaseUnquotedNameConverter

  override
  val addingForeignKeyConstraintCreatesIndex = true

  override
  def columnDefinitionFactory
    (column_type: SqlType,
     character_set_opt: Option[CharacterSet]): ColumnDefinition =
  {
    character_set_opt match {
      case None =>
      case Some(CharacterSet(Unicode)) =>
      case Some(set @ CharacterSet(_)) => {
        logger.warn("Ignoring '{}' as Derby uses Unicode sequences to " +
                    "represent character data types.",
                    set)
      }
    }

    column_type match {
      case BigintType =>
        new DefaultBigintColumnDefinition
      case BlobType =>
        new DefaultBlobColumnDefinition
      case BooleanType => {
        val message = "Derby does not support a boolean type, you must " +
                      "choose a mapping your self."
        throw new UnsupportedColumnTypeException(message)
      }
      case CharType =>
        new DefaultCharColumnDefinition
      case DecimalType =>
        new DefaultDecimalColumnDefinition
      case IntegerType =>
        new DefaultIntegerColumnDefinition
      case TimestampType =>
        new DerbyTimestampColumnDefinition
      case SmallintType =>
        new DefaultSmallintColumnDefinition
      case VarbinaryType =>
        new DerbyVarbinaryColumnDefinition
      case VarcharType =>
        new DefaultVarcharColumnDefinition
    }
  }

  override protected
  def alterColumnSql(schema_name_opt: Option[String],
                     column_definition: ColumnDefinition): String =
  {
    new java.lang.StringBuilder(512)
      .append("ALTER TABLE ")
      .append(quoteTableName(schema_name_opt, column_definition.getTableName))
      .append(" ALTER ")
      .append(quoteColumnName(column_definition.getColumnName))
      .append(" SET DATA TYPE ")
      .append(column_definition.toSql)
      .toString
  }
}
