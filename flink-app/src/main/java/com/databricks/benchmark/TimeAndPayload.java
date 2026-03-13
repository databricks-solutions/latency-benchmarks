/*
 * Copyright 2026 Databricks, Inc.
 *
 * This work (the "Licensed Material") is licensed under the Creative Commons
 * Attribution-NonCommercial-NoDerivatives 4.0 International License. You may
 * not use this file except in compliance with the License.
 *
 * To view a copy of this license, visit https://creativecommons.org/licenses/by-nc-nd/4.0/
 *
 * Unless required by applicable law or agreed to in writing, the Licensed Material is offered
 * on an "AS-IS" and "AS-AVAILABLE" BASIS, WITHOUT REPRESENTATIONS OR WARRANTIES OF ANY KIND,
 * whether express, implied, statutory, or other. This includes, without limitation, warranties
 * of title, merchantability, fitness for a particular purpose, non-infringement, absence of
 * latent or other defects, accuracy, or the presence or absence of errors, whether or not known
 * or discoverable. To the extent legally permissible, in no event will the Licensor be liable
 * to You on any legal theory (including, without limitation, negligence) or otherwise for
 * any direct, special, indirect, incidental, consequential, punitive, exemplary, or other
 * losses, costs, expenses, or damages arising out of this License or use of the Licensed
 * Material, even if the Licensor has been advised of the possibility of such losses, costs,
 * expenses, or damages.
 */

package com.databricks.benchmark;

import java.util.Arrays;
import java.util.Objects;

public class TimeAndPayload {
  public long time;
  public byte[] payload;

  public TimeAndPayload() {}

  public TimeAndPayload(long time, byte[] payload) {
    this.time = time;
    this.payload = payload;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TimeAndPayload that = (TimeAndPayload) o;
    return time == that.time && Arrays.equals(payload, that.payload);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(time);
    result = 31 * result + Arrays.hashCode(payload);
    return result;
  }
}
